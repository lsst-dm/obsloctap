""" Return some dummy entires for testing"""

from obsloctap.efd import efd_help
from obsloctap.models import Observation


class mock_efd_help(efd_help):
    def getSchedule(self) -> list[Observation]:
        observations = []
        obs = Observation(
            mjd="60032.194918981484",
            ra=90.90909091666666,
            dec=-74.60384434722222,
            rotSkyPos=18.33895879413964,
            nexp=3,
        )
        observations.append(obs)
        return observations
