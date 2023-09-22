from typing import Optional, ItemsView


class ValidationResult:
    def __init__(self, prop: Optional[object] = None, message: Optional[str] = ''):
        self.prop = prop
        self.message = message

    def __str__(self):
        return self.message

    def __bool__(self):
        raise NotImplementedError


class ValidationFailure(ValidationResult):
    def __bool__(self):
        return False


class ValidationSuccess(ValidationResult):
    def __bool__(self):
        return True


class ValidationResultsDict(dict):
    @property
    def ok(self):
        return len(self.failures()) == 0

    def failures(self) -> ItemsView[str, ValidationFailure]:
        return {k: v for k, v in self.items() if isinstance(v, ValidationFailure)}.items()

    def successes(self) -> ItemsView[str, ValidationSuccess]:
        return {k: v for k, v in self.items() if isinstance(v, ValidationSuccess)}.items()
