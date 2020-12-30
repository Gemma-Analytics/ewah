from wtforms import widgets


class EWAHTextAreaWidget(widgets.TextArea):
    """Make a variable-depth Text Area widget."""

    def __init__(self, rows=3):
        self.rows = rows
        return super().__init__()

    def __call__(self, field, **kwargs):
        kwargs["class"] = u"form-control"
        kwargs["rows"] = kwargs.get("rows", self.rows)
        if field.label:
            kwargs["placeholder"] = field.label.text
        return super().__call__(field, **kwargs)
