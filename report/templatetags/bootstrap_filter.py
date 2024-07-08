# my_app/templatetags/bootstrap_filter.py
from django import template

register = template.Library()

@register.filter(name='as_bootstrap')
def as_bootstrap(value):
    return value.as_widget(attrs={'class': 'form-control'})
