def as_list(children):
    """Return untangle children as a list (a single child is a bare Element)."""
    if children is None:
        return []
    if isinstance(children, list):
        return children
    return [children]
