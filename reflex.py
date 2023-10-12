from importlib import import_module
from secrets import token_urlsafe
from django.conf import settings
from django.template.loader import render_to_string
from django.template.backends.django import Template
from django.urls import resolve
from urllib.parse import urlparse

from django.test import RequestFactory

from .channel import Channel

PROTECTED_VARIABLES = [
    'consumer',
    'element',
    'is_morph',
    'selectors',
    'session',
    'url',
]


class Reflex:
    def __init__(
        self, consumer, url, element, selectors, params, identifier='',
        permanent_attribute_name=None, reflex_id=None, data=None
    ):
        self.consumer = consumer
        self.url = url
        self.element = element
        self.selectors = selectors
        self.session = consumer.scope["session"]
        self.params = params
        self.identifier = identifier
        self.is_morph = False
        self.reflex_id = reflex_id
        self.permanent_attribute_name = permanent_attribute_name
        self.context = {}
        self.data = data or {}

    def __repr__(self):
        return f"<Reflex url: {self.url}, session: {self.get_channel_id()}>"

    def get_context_data(self, *args, **kwargs):
        #if self.context:
        #    self.context.update(**kwargs)
        #    return self.context

        parsed_url = urlparse(self.url)
        resolved = resolve(parsed_url.path)
        view = resolved.func.view_class()
        view.request = self.request
        view.kwargs = resolved.kwargs

        # correct for detail and list views for django generic views
        if hasattr(view, "get_object"):
            view.object = view.get_object()

        if hasattr(view, "paginate_queryset"):
            view.object_list = view.get_queryset()

        kwargs.update(view.kwargs)
        context = view.get_context_data(**{"stimulus_reflex": True, **kwargs})

        for context_processor in settings.TEMPLATES[0]['OPTIONS']['context_processors']:
            split = context_processor.split('.')
            module, klassname = split[:-1], split[-1]
            module = '.'.join(module)
            func = getattr(import_module(module), klassname)
            context.update(func(view.request))

        if context.get('user') and context.get('user').id:
            context.get('user').refresh_from_db()

        self.context = context
        self.context.update(**kwargs)

        return self.context

    def get_channel_id(self):
        """
        Override this to make the reflex send to a different channel
        other than the session_key of the user
        """
        return self.session.session_key

    @property
    def request(self):
        factory = RequestFactory()
        request = factory.get(self.url)
        request.session = self.consumer.scope["session"]
        request.user = self.consumer.scope["user"]
        request.POST = self.params
        return request

    def reload(self):
        """A default reflex to force a refresh"""
        pass

    def get_channel_id(self):
        '''
        Override this to make the reflex send to a different channel
        other than the session_key of the user
        '''
        return self.session.session_key

    def morph(self, selector='', html=None, template='', context={}, select_all=False):
        """
        If a morph is executed without any arguments, nothing is executed
        and the reflex won't send over any data to the frontend.
        """
        self.is_morph = True
        broadcaster = Channel(self.consumer.channel_name, identifier=self.identifier)

        no_arguments = [not selector, html == None, (not template and not context)]
        if all(no_arguments) and not selector:
            # an empty morph, dispatches an event with the name 'empty_morph', which does nothing.
            broadcaster.dispatch_event({
                'name': 'empty_morph',
                'detail': {
                    'stimulus_reflex': {
                        'reflexId': self.reflex_id,
                        'url': self.url
                    }
                },
            })
        else:
            if html != None:
                html = html
            elif isinstance(template, Template):
                html = template.render(context)
            else:
                html = render_to_string(template, context)

            broadcaster.morph({
                'selector': selector,
                'html': html,
                'children_only': True,
                'select_all': select_all,
                'permanent_attribute_name': self.permanent_attribute_name,
                'stimulus_reflex': {
                    'morph': 'selector',
                    'reflexId': self.reflex_id,
                    'url': self.url
                }
            })

        broadcaster.broadcast()

    def send_toast(self, toast_context, detail:dict):
        """detail gets piped into bootstrap toast options can be either autohide:False or delay:(ms) """
        from django.template import Context, Template as Template_django
        broadcaster = Channel(self.consumer.channel_name, identifier=self.identifier)

        element_id = 't' + token_urlsafe(4)
        toast_context.update(id=element_id)
        if 'body' in toast_context:
            t = Template_django(toast_context['body'])
            toast_context['body'] = t.render(Context(toast_context))
        html = render_to_string('toast.html', context=toast_context)

        broadcaster.insert_adjacent_html({'selector':'.toast-container', 'html':html})
        broadcaster.broadcast()
        detail_args = {'id':element_id,}
        detail_args.update(detail)
        broadcaster.dispatch_event({'name': 'toast','detail': detail_args})
        broadcaster.broadcast()