import inspect
import json
import logging
import time
from functools import wraps
from importlib import import_module
from os import path, walk
from urllib.parse import parse_qsl, urlparse

import pyinstrument
from asgiref.sync import async_to_sync
from channels.generic.websocket import JsonWebsocketConsumer
from django.apps import apps
from django.conf import settings
from django.urls import resolve
from django.utils import timezone
from django_rq import get_connection

from .channel import Channel
from .element import Element
from .reflex import PROTECTED_VARIABLES, Reflex
from .utils import get_document_and_selectors, parse_out_html

# from siu_trace import traceit


logger = logging.getLogger("sockpuppet")


class SockpuppetError(Exception):
    pass


def context_decorator(method, extra_context):
    @wraps(method)
    def wrapped(self, *method_args, **method_kwargs):
        method_kwargs.update(extra_context)
        context = method(self, *method_args, **method_kwargs)
        # if context was picked from cache extra context needs to be added again
        context.update(extra_context)
        return context

    return wrapped


class BaseConsumer(JsonWebsocketConsumer):
    reflexes = {}
    subscriptions = set()

    def _get_channelname(self, channel_name):
        try:
            # StimulusReflex sends the channel name in the format
            # of a json blob for name.
            name = json.loads(channel_name)
            name = name["channel"].replace("::", "-")
        except json.decoder.JSONDecodeError:
            name = channel_name
        return name

    def connect(self):
        """
        We use the user session key as a default channel to publish any events
        """
        super().connect()

        # if 'session' not in self.scope:
        #     self.websocket_disconnect('you have no power here')
        #     return

        session = self.scope["session"]
        has_session_key = session.session_key

        if not has_session_key:
            # normally there is no session key for anonymous users.
            session.save()

        if settings.DATABASES.get("default", {}).get("NAME", "") == "buttler":
            origin = next(
                (v.decode() for k, v in self.scope["headers"] if k == b"origin"), None
            )
            if origin and "buttler" not in origin:
                session.set_expiry(3600)
                session.save()

        async_to_sync(self.channel_layer.group_add)(
            session.session_key, self.channel_name
        )

        if not has_session_key:
            self.group_send(
                self.scope["session"].session_key,
                {
                    "type": "message",
                    "meta_type": "cookie",
                    "key": "sessionid",
                    "value": session.session_key,
                    "max_age": 3600,
                },
            )

        logger.debug(
            ":: CONNECT: Channel %s session: %s", self.channel_name, session.session_key
        )

    def disconnect(self, *args, **kwargs):
        """
        When we disconnect we unsubscribe from the user session key.
        """

        if "session" in self.scope:
            import redis

            r_channels = redis.Redis(db=0)
            keys = r_channels.keys("asgi:group:*")
            for key in keys:
                key = key.decode()
                async_to_sync(self.channel_layer.group_discard)(
                    key.split(":")[2], self.channel_name
                )
            session = self.scope["session"]
            async_to_sync(self.channel_layer.group_discard)(
                session.session_key, self.channel_name
            )
            logger.debug(
                ":: DISCONNECT: Channel %s session: %s",
                self.channel_name,
                session.session_key,
            )
        super().disconnect(*args, **kwargs)

    def subscribe(self, data, **kwargs):
        name = self._get_channelname(data["channelName"])
        name = name.replace("=", "-").replace("%", "-")
        logger.debug("Subscribe %s to %s", self.channel_name, name)
        async_to_sync(self.channel_layer.group_add)(name, self.channel_name)

    def unsubscribe(self, data, **kwargs):
        if "channelName" not in data:
            return
        name = self._get_channelname(data["channelName"])
        name = name.replace("=", "-").replace("%", "-")
        logger.debug("Unsubscribe %s from %s", self.channel_name, name)
        async_to_sync(self.channel_layer.group_discard)(name, self.channel_name)

    def receive_json(self, data, **kwargs):
        message_type = data.get("type")
        redis_connection = get_connection()
        if message_type is None and data.get("target"):
            # 1. sentry transactions:
            # with sentry_sdk.start_transaction(op="reflex", name=f"{data.get('url').replace(settings.BASE_URL, '')} {data.get('target')}"):
            #     self.reflex_message(data, **kwargs)

            # 2. py trace
            # import sys
            # import threading

            # def traceit(frame, event, arg):
            #     print(event, frame.f_lineno)
            #     return traceit
            # threading.settrace(traceit)
            # sys.settrace(traceit)
            # try:
            #     self.reflex_message(data, **kwargs)
            # finally:
            #     sys.settrace(None)
            #     threading.settrace(None)

            name = f"{data.get('target')}"

            cpu_counter = time.process_time()
            time_counter = time.perf_counter()

            pyinstrument_profiler = None
            if redis_connection.hexists(
                "siu:perf:pyinstrument_list", name
            ) or redis_connection.get("siu:perf:pyinstrument_reflexes"):
                interval = float(
                    redis_connection.get("siu:perf:pyinstrument_reflexes_interval")
                    or 0.1
                )
                pyinstrument_profiler = pyinstrument.Profiler(interval=interval)
                pyinstrument_profiler.start()

            self.reflex_message(data, **kwargs)

            if pyinstrument_profiler and pyinstrument_profiler.is_running:
                pyinstrument_session = pyinstrument_profiler.stop()
                if pyinstrument_session.sample_count > 0:
                    pyinstrument_output = pyinstrument_profiler.output_text(
                        show_all=True
                    )
                    logger.warning("pyinstrument %s\n  %s", name, pyinstrument_output)
                    redis_connection.set(
                        f"siu:perf:reflexes:{name}:pyinstrument", pyinstrument_output
                    )

            cpu_time = (time.process_time() - cpu_counter) * 1000
            wall_time = (time.perf_counter() - time_counter) * 1000

            redis_connection.set(f"siu:perf:reflexes:{name}:cpu_time", cpu_time)
            redis_connection.set(f"siu:perf:reflexes:{name}:wall_time", wall_time)

            date = timezone.now().date().isoformat()

            if not redis_connection.exists(f"siu:perf:reflexes:{name}:{date}"):
                redis_connection.hset(f"siu:perf:reflexes:{name}:{date}", "count", 0)
                redis_connection.expire(f"siu:perf:reflexes:{name}:{date}", 604800)

            redis_connection.hincrby(f"siu:perf:reflexes:{name}:{date}", "count", 1)
            redis_connection.hincrbyfloat(
                f"siu:perf:reflexes:{name}:{date}", "cpu_time_total", cpu_time
            )
            redis_connection.hincrbyfloat(
                f"siu:perf:reflexes:{name}:{date}", "wall_time_total", wall_time
            )

            if wall_time_min_raw := redis_connection.hget(
                f"siu:perf:reflexes:{name}:{date}", "wall_time_min"
            ):
                if wall_time < float(wall_time_min_raw.decode()):
                    redis_connection.hset(
                        f"siu:perf:reflexes:{name}:{date}", "wall_time_min", wall_time
                    )
            else:
                redis_connection.hset(
                    f"siu:perf:reflexes:{name}:{date}", "wall_time_min", wall_time
                )

            if wall_time_max_raw := redis_connection.hget(
                f"siu:perf:reflexes:{name}:{date}", "wall_time_max"
            ):
                if wall_time > float(wall_time_max_raw.decode()):
                    redis_connection.hset(
                        f"siu:perf:reflexes:{name}:{date}", "wall_time_max", wall_time
                    )
            else:
                redis_connection.hset(
                    f"siu:perf:reflexes:{name}:{date}", "wall_time_max", wall_time
                )

            if cpu_time_min_raw := redis_connection.hget(
                f"siu:perf:reflexes:{name}:{date}", "cpu_time_min"
            ):
                if cpu_time < float(cpu_time_min_raw.decode()):
                    redis_connection.hset(
                        f"siu:perf:reflexes:{name}:{date}", "cpu_time_min", cpu_time
                    )
            else:
                redis_connection.hset(
                    f"siu:perf:reflexes:{name}:{date}", "cpu_time_min", cpu_time
                )

            if cpu_time_max_raw := redis_connection.hget(
                f"siu:perf:reflexes:{name}:{date}", "cpu_time_max"
            ):
                if cpu_time > float(cpu_time_max_raw.decode()):
                    redis_connection.hset(
                        f"siu:perf:reflexes:{name}:{date}", "cpu_time_max", cpu_time
                    )
            else:
                redis_connection.hset(
                    f"siu:perf:reflexes:{name}:{date}", "cpu_time_max", cpu_time
                )

            logger.warning(
                f"reflex {data.get('target')} cpu: %6.2fms, wall: %6.2fms",
                cpu_time,
                wall_time,
            )

        elif message_type == "subscribe":
            self.subscribe(data, **kwargs)
        elif message_type == "unsubscribe":
            self.unsubscribe(data, **kwargs)
        else:
            print("Unsupported")

    def message(self, event):
        logger.debug("Sending data: %s", event)
        self.send(json.dumps(event))

    def group_send(self, recipient, message):
        send = async_to_sync(self.channel_layer.group_send)
        send(recipient, message)

    def load_reflexes(self):
        configs = apps.app_configs.values()
        for config in configs:
            self.load_reflexes_from_config(config)

    def load_reflexes_from_config(self, config):
        def append_reflex():
            self.reflexes.update(
                {
                    ReflexClass.__name__: ReflexClass
                    for ReflexClass in Reflex.__subclasses__()
                }
            )

        modpath = config.module.__path__[0]

        for dirpath, dirnames, filenames in walk(modpath):
            if dirpath == modpath and "reflexes.py" in filenames:
                # classes in reflexes.py
                import_path = "{}.reflexes".format(config.name)
                import_module(import_path)
                append_reflex()

            elif dirpath == path.join(modpath, "reflexes"):
                # assumes reflexes folder is placed directly in app.
                import_path = "{config_name}.reflexes.{reflex_file}"

                for filename in filenames:
                    # eliminates empty values in the filename before getting the
                    # module name from the filename.
                    name = [file for file in filename.split(".") if file][0]
                    full_import_path = import_path.format(
                        config_name=config.name, reflex_file=name
                    )
                    import_module(full_import_path)
                    append_reflex()

    def reflex_message(self, data, **kwargs):
        logger.debug("RECEIVED Json: %s", data)
        logger.debug("RECEIVED kwargs: %s", kwargs)

        if settings.DEBUG:
            import os

            filename = data["target"].replace("/", "_")
            os.makedirs("/tmp/reflex_message/", exist_ok=True)
            with open(
                f"/tmp/reflex_message/{filename}.txt", "w", encoding="utf-8"
            ) as file:
                json.dump(data, file)

        url = data["url"]
        selectors = data["selectors"] if data["selectors"] else ["body"]
        target = data["target"]
        identifier = data["identifier"]
        try:
            reflex_class_name, method_name = target.split("#")
        except:
            logger.warning(f"reflex_message cannot split [{target}]")
        arguments = data["args"] if data.get("args") else []
        params = dict(parse_qsl(data["formData"]))
        element = Element(data["attrs"])

        try:
            if not self.reflexes:
                self.load_reflexes()
        except Exception as e:
            msg = f"Reflex couldn't be loaded: {str(e)}"
            self.broadcast_error(msg, data)
            logger.warning((msg, data))
            return

        # try:
        #     if settings.DATABASES.get('default', {}).get('NAME', '') == 'buttler':
        #         # load them bitches:
        #         from django.db import connection
        #         with connection.cursor() as cursor:
        #             cursor.execute("select d_string, name, id from fafo_block where meta_data_id=4;", [])
        #             for row in cursor.fetchall():
        #                 try:
        #                     exec(f'{row[0]}\n')
        #                 except Exception as e:
        #                     logger.error(f'Failed to load {row[2]}.{row[1]}\n{e}')
        #                 self.reflexes.update(
        #                     {
        #                         ReflexClass.__name__: ReflexClass
        #                         for ReflexClass in Reflex.__subclasses__()
        #                     }
        #                 )
        # except Exception as e:
        #     msg = f"Reflex couldn't be loaded: {str(e)}"
        #     self.broadcast_error(msg, data)
        #     return

        try:
            # TODO(danilo): this whole class ordeal is ridiculous, purge the same way the js controllers got purged

            ReflexClass = self.reflexes.get(reflex_class_name)
            reflex = ReflexClass(
                self,
                url=url,
                element=element,
                selectors=selectors,
                identifier=identifier,
                params=params,
                reflex_id=data["reflexId"],
                data=data,
            )
            self.delegate_call_to_reflex(reflex, method_name, arguments)
            if reflex.session is not None:
                reflex.session.save()
        except TypeError as exc:
            if not self.reflexes.get(reflex_class_name):
                msg = f"Sockpuppet tried to find a reflex class called {reflex_class_name}. Are you sure such a class exists?"  # noqa
                self.broadcast_error(msg, data)
            else:
                msg = str(exc)
                self.broadcast_error(msg, data)
            logging.exception(msg)
            return
        except Exception as e:
            error = "{}: {}".format(e.__class__.__name__, str(e))
            msg = "SockpuppetConsumer failed to invoke {target}, with url {url}. {message}".format(
                target=target, url=url, message=error
            )
            self.broadcast_error(msg, data, None)
            logging.exception(msg)
            return

        try:
            self.render_page_and_broadcast_morph(reflex, selectors, data)
        except Exception as e:
            error = "{}: {}".format(e.__class__.__name__, str(e))
            msg = "SockpuppetConsumer failed to re-render {url} {message}".format(
                url=url, message=error
            )
            self.broadcast_error(msg, data, reflex)
            logging.exception(msg)
            return

    def render_page_and_broadcast_morph(self, reflex, selectors, data):
        if reflex.is_morph:
            # The reflex has already sent a message so consumer doesn't need to.
            return

        html = self.render_page(reflex)
        if html:
            self.broadcast_morphs(selectors, data, html, reflex)

    def render_page(self, reflex):
        parsed_url = urlparse(reflex.url)
        resolved = resolve(parsed_url.path)
        view = resolved.func

        instance_variables = [
            name
            for (name, member) in inspect.getmembers(reflex)
            if not name.startswith("__") and name not in PROTECTED_VARIABLES
        ]
        reflex_context = {key: getattr(reflex, key) for key in instance_variables}
        reflex_context["stimulus_reflex"] = True

        # original_context_data = view.view_class.get_context_data
        # reflex.get_context_data(**reflex_context)
        # # monkey patch context method
        # view.view_class.get_context_data = reflex.get_context_data
        # # We also need to make sure that the last update from reflex context wins
        # view.view_class.get_context_data = context_decorator(
        #     view.view_class.get_context_data, reflex_context
        # )

        request = reflex.request
        request.META["HTTP_HOST"] = settings.SERVER_NAME
        response = view(request, *resolved.args, **resolved.kwargs)
        # we've got the response, the function needs to work as normal again
        # view.view_class.get_context_data = original_context_data
        reflex.session.save()
        return response.rendered_content

    def broadcast_morphs(self, selectors, data, html, reflex):
        document, selectors = get_document_and_selectors(html, selectors)

        # channel = Channel(reflex.get_channel_id(), identifier=data["identifier"])
        # logger.debug("Broadcasting to %s", reflex.get_channel_id())

        channel = Channel(self.channel_name, identifier=data["identifier"])
        logger.debug("Broadcasting to %s", self.channel_name)

        # TODO can be removed once stimulus-reflex has increased a couple of versions
        permanent_attribute_name = data.get("permanent_attribute_name")
        if not permanent_attribute_name:
            # Used in stimulus-reflex >= 3.4
            permanent_attribute_name = data["permanentAttributeName"]

        for selector in selectors:
            # cssselect has an attribute css
            plain_selector = getattr(selector, "css", selector)
            channel.morph(
                {
                    "selector": plain_selector,
                    "html": parse_out_html(document, selector),
                    "children_only": True,
                    "permanent_attribute_name": permanent_attribute_name,
                    "stimulus_reflex": {**data},
                }
            )
        channel.broadcast()

    def delegate_call_to_reflex(self, reflex, method_name, arguments):
        method = getattr(reflex, method_name)
        method_signature = inspect.signature(method)
        if len(method_signature.parameters) == 0:
            getattr(reflex, method_name)()
        else:
            getattr(reflex, method_name)(*arguments)

    def broadcast_error(self, message, data, reflex=None):
        # We may have a situation where we weren't able to get a reflex
        session_key = (
            reflex.get_channel_id() if reflex else self.scope["session"].session_key
        )
        if not "identifier" in data:
            data["identifier"] = reflex.identifier if reflex else session_key
        channel = Channel(session_key, identifier=data["identifier"])
        data.update(
            {
                "serverMessage": {
                    "subject": "error",
                    "body": message,
                }
            }
        )
        channel.dispatch_event(
            {
                "name": "stimulus-reflex:server-message",
                "detail": {"stimulus_reflex": data},
            }
        )
        channel.broadcast()

    def broadcast_server_data(self, data, reflex=None):
        session_key = (
            reflex.get_channel_id() if reflex else self.scope["session"].session_key
        )

        stimulus_data = {}

        if not "identifier" in data:
            stimulus_data["identifier"] = reflex.identifier if reflex else session_key
        else:
            stimulus_data["identifier"] = data.pop("identifier")

        if reflex:
            stimulus_data["reflexId"] = reflex.reflex_id

        channel = Channel(session_key, identifier=stimulus_data["identifier"])

        channel.data(
            {
                "name": "data",
                "detail": {"stimulus_reflex": stimulus_data},
                "response": data,
            }
        )
        channel.broadcast()


class SockpuppetConsumer(BaseConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not self.reflexes:
            self.load_reflexes()


class SockpuppetConsumerAsgi(BaseConsumer):
    """
    This consumer supports the asgi standard now in django
    This consumer should be used when using channels 3.0.0 and upwards
    """

    async def __call__(self, scope, receive, send):
        await super().__call__(scope, receive, send)

        if not self.reflexes:
            self.load_reflexes()
