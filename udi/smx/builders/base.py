class SmxComponent:
    """Temporary proxy used during the structural-refactor phase.

    Reads and writes are forwarded to the existing SMX facade so moved code can
    keep using the same ``self.<attribute>`` expressions. This is intentionally
    transitional and can be replaced by explicit dependencies during the later
    SOLID redesign.
    """

    def __init__(self, smx):
        object.__setattr__(self, "smx", smx)

    def __getattr__(self, name):
        return getattr(self.smx, name)

    def __setattr__(self, name, value):
        if name == "smx" or "smx" not in self.__dict__:
            object.__setattr__(self, name, value)
        else:
            setattr(self.smx, name, value)
