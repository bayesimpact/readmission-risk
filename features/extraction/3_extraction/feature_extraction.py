"""Feature extraction pipeline for sutter."""

import logging

from feature_extractors.admission import AdmissionExtractor
from feature_extractors.comorbidities import ComorbiditiesExtractor
from feature_extractors.demographics import BasicDemographicsExtractor
from feature_extractors.discharge import DischargeExtractor
from feature_extractors.encounter_reason import EncounterReasonExtractor
from feature_extractors.health_history import HealthHistoryExtractor
from feature_extractors.hospital_problems import HospitalProblemsExtractor
from feature_extractors.lab_results import LabResultsExtractor
from feature_extractors.labels import ReadmissionExtractor
from feature_extractors.medications import MedicationsExtractor
from feature_extractors.payer import PayerExtractor
from feature_extractors.procedures import ProceduresExtractor
from feature_extractors.provider import ProviderExtractor
from feature_extractors.socioeconomic import SocioeconomicExtractor
from feature_extractors.utilization import UtilizationExtractor
from feature_extractors.vitals import VitalsExtractor

from fex import runner

logging.basicConfig(format='%(levelname)s:%(name)s:%(asctime)s=> %(message)s',
                    datefmt='%m/%d %H:%M:%S',
                    level=logging.INFO)


feature_extractors = [
    ReadmissionExtractor(),
    BasicDemographicsExtractor(),
    EncounterReasonExtractor(),
    UtilizationExtractor(),
    DischargeExtractor(),
    ProviderExtractor(),
    PayerExtractor(),
    AdmissionExtractor(),
    HealthHistoryExtractor(),
    ComorbiditiesExtractor(),  # temporarily disabling (too slow)
    ProceduresExtractor(),
    HospitalProblemsExtractor(),
    VitalsExtractor(),
    MedicationsExtractor(),
    LabResultsExtractor(),
    SocioeconomicExtractor()
]

if __name__ == '__main__':
    runner.run(*feature_extractors, args=["--no-cache"])
