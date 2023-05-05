"""Helper fr the efd -so it may be mocked in test."""

__all__ = ["efd_help"]

from obsloctap.models import Observation


class efd_help:
    def getSchedule(self) -> list[Observation]:
        """Return the latest schedule item from the EFD"""
        obs = Observation(
            mjd="0",
            ra=180,
            dec=-90,
            rotSkyPos=0,
            nexp=1,
        )
        obslist = [obs]
        return obslist
