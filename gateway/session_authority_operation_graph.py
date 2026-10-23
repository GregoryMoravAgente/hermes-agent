"""Identity-bound producer graph for session-authority operation custody."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PreservationProducerGraph:
    """Typed bootstrap witness for authority-local producer indexes.

    The graph is an identity witness, not a second custody ledger. Each entry retains the exact
    container created by ``SessionAuthority`` so readiness can distinguish an explicitly
    registered empty source from an absent, replaced, or malformed source index.
    """

    authority: object
    profile_id: str
    instance_id: str
    authority_epoch: int
    indexes: tuple[tuple[str, object, type], ...]
    coordinator: object | None = None

    def matches(self, authority, coordinator) -> bool:
        if (
            self.authority is not authority
            or self.coordinator is not coordinator
            or getattr(authority, 'runner', None) is None
            or getattr(authority.runner, 'runtime_custody', None) is not coordinator
            or getattr(authority, 'profile_id', None) != self.profile_id
            or getattr(authority, 'instance_id', None) != self.instance_id
            or getattr(authority, 'epoch', None) != self.authority_epoch
            or getattr(coordinator, 'process_instance', None) != self.instance_id
        ):
            return False
        sentinel = object()
        for name, expected, expected_type in self.indexes:
            current = getattr(authority, name, sentinel)
            if current is not expected or type(current) is not expected_type:
                return False
        return True
