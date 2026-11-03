"""Jobs Silver, dans l'ordre d'exécution imposé par les dépendances :
référentiels d'abord (COG, établissements), puis entités, puis faits."""
from . import (communes, consultations, deces, diagnostics, etablissements, hospitalisations,
               patients, professionnels, satisfaction)

ORDER = [communes, etablissements, professionnels, diagnostics, patients,
         consultations, hospitalisations, satisfaction, deces]
JOBS = {m.TABLE: m for m in ORDER}
