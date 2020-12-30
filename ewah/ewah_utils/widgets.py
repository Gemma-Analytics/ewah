from wtforms import widgets

class EWAHTextAreaWidget(widgets.TextArea):
    """Make a variable-depth Text Area widget."""

    def __call__(self, field, **kwargs):
        kwargs["class"] = u"form-control"
        kwargs["rows"] = kwargs.get("rows", 3)
        if field.label:
            kwargs["placeholder"] = field.label.text
        return super().__call__(field, **kwargs)
