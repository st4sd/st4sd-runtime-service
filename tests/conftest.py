# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import base64
import logging
import shutil
import tempfile
import uuid
from typing import Dict
from typing import List
from typing import TYPE_CHECKING

import experiment.model.storage
import kubernetes.client
import pytest

import apis.models.virtual_experiment
import apis.storage
from apis.models.constants import *

if TYPE_CHECKING:
    import experiment.model.data


FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
logging.basicConfig(format=FORMAT)
rootLogger = logging.getLogger()


@pytest.fixture()
def flowir_psi4() -> str:
    return """
application-dependencies: {}
virtual-environments: {}
status-report:
  0:
    arguments: '1'
    executable: echo
    stage-weight: 1.0
output: 
  Energies:
    data-in: aggregateEnergies/all_energies.csv:copy
    description: '"Molecular energies from ase-ani optimized molecules"'
    stages:
      - 0
    type: csv
# Need an aggrgate component to run this
#  
#  Optimized_geometry:
#    data-in: AniOptimize/optimized.xyz:copy
#    description: '"Optimized molecular structures from ase-ani optimized molecules"'
#    stages:
#      - 0
#    type: xyz

platforms:
- default
- openshift
environments:
  default:
    environment:
      OMP_NUM_THREADS: '4'
    PYTHON: {}

components: 
# Replicate over the number of molcules given
- stage: 0
  name: GetMoleculeIndex
  command:
    arguments: -c \"print(%(startIndex)s + %(replica)s),\"
    executable: python
  workflowAttributes:
    replicate: '%(numberMolecules)s'

# This component builds a 3D geometry from a smiles string and optimizes it with a force field (UFF) 
# It then runs each ANI model and calculates energy at the force field minimum. Following that the mean
# and standard deviation of the energies are computed. The mean is reported as the energy and the 
# standard deviation the uncertainty. The uncertainty is written to the file uncertaininty.txt for a decision
# to be made about changing the components. The energy is saved in energies.csv.
- stage: 0
  name: ForceFieldOptANIUncertainty
  command:
    arguments: -scsv input/smiles.csv:ref -ri GetMoleculeIndex:output
    environment: python
    executable: bin/optimize_ff.py
  references:
  - input/smiles.csv:ref
  - GetMoleculeIndex:output
  resourceManager:
    config:
      backend: '%(backend)s'
    kubernetes:
      image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/ani-torch-psi4-st4sd:0.1.0
    lsf:
      queue: normal


# This component used ANI neural potential to optimize the molecule structure through the ASE from the
# force field minimum in the last stage. The molecule is loaded from optimized.xyz in the previous stage.
- stage: 0
  name: Psi4Optimize
  command:
    arguments: -xyz ForceFieldOptANIUncertainty/optimized.xyz:ref -rk %(rep_key)s -ri GetMoleculeIndex:output -o %(optimizer)s -i %(max_opt_steps)s --temperature %(T)s --pressure %(P)s --force_tolerance %(force_tolerance)s --ff_minimize 0 -amac 0
    environment: python
    executable: bin/optimize_psi4.py
  references:
  - input/smiles.csv:copy
  - GetMoleculeIndex:output
  - ForceFieldOptANIUncertainty/optimized.xyz:ref
  resourceManager:
    config:
      backend: '%(backend)s'
    kubernetes:
      image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/ani-torch-psi4-st4sd:0.1.0
    lsf:
      queue: normal

# Pull all of the energies together in one file over all mol
- stage: 0
  name: aggregateEnergies
  command:
    arguments: -f Psi4Optimize/energies.csv:ref
    environment: python
    executable: bin/aggregate_energies.py
  references:
  - Psi4Optimize/energies.csv:ref
  workflowAttributes:
    aggregate: true
  resourceManager:
    config:
      backend: '%(backend)s'
    kubernetes:
      image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/caf-st4sd:1.0.0
    lsf:
      queue: normal
variables:
  default:
    global:
      xyzfile: "none"
      smiles_csv: "smiles.csv"
      inchi_csv: "none"
      rep_key: "smiles"
      ani_model: "ani2x"
      optimizer: "bfgs"
      max_opt_steps: 5000
      force_tolerance: 0.005
      T: 298.15
      P: 101325.0
      numberMolecules: 1
      startIndex: 0
      defaultq: "normal"
      ff_minimize: 1
      ani_minimize_all_conformers: 0
  openshift:
    global:
      backend: kubernetes
      number-processors: '1'
    stages:
      0:
        stage-name: ani_optimization
version: 0.2.0
"""


@pytest.fixture()
def flowir_neural_potential() -> str:
    return """
application-dependencies: {}
virtual-environments: {}
status-report:
  0:
    arguments: '1'
    executable: echo
    stage-weight: 1.0
output: 
  Energies:
    data-in: aggregateEnergies/all_energies.csv:copy
    description: '"Molecular energies from ase-ani optimized molecules"'
    stages:
      - 0
    type: csv
# Need an aggrgate component to run this
#  
#  Optimized_geometry:
#    data-in: AniOptimize/optimized.xyz:copy
#    description: '"Optimized molecular structures from ase-ani optimized molecules"'
#    stages:
#      - 0
#    type: xyz

platforms:
- default
- openshift
environments:
  default:
    environment:
      OMP_NUM_THREADS: '4'
    PYTHON: {}

components: 
# Replicate over the number of molcules given
- stage: 0
  name: GetMoleculeIndex
  command:
    arguments: -c \"print(%(startIndex)s + %(replica)s),\"
    executable: python
  workflowAttributes:
    replicate: '%(numberMolecules)s'

# This component builds a 3D geometry from a smiles string and optimizes it with a force field (UFF) 
# It then runs each ANI model and calculates energy at the force field minimum. Following that the mean
# and standard deviation of the energies are computed. The mean is reported as the energy and the 
# standard deviation the uncertainty. The uncertainty is written to the file uncertaininty.txt for a decision
# to be made about changing the components. The energy is saved in energies.csv.
- stage: 0
  name: ForceFieldOptANIUncertainty
  command:
    arguments: -scsv input/smiles.csv:ref -ri GetMoleculeIndex:output
    environment: python
    executable: bin/optimize_ff.py
  references:
  - input/smiles.csv:ref
  - GetMoleculeIndex:output
  resourceManager:
    config:
      backend: '%(backend)s'
    kubernetes:
      image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/ani-torch-psi4-st4sd:0.1.0
    lsf:
      queue: normal


# This component used ANI neural potential to optimize the molecule structure through the ASE from the
# force field minimum in the last stage. The molecule is loaded from optimized.xyz in the previous stage.
- stage: 0
  name: AniOptimize
  command:
    arguments: -xyz ForceFieldOptANIUncertainty/optimized.xyz:ref  -rk %(rep_key)s -ri GetMoleculeIndex:output --ani_model %(ani_model)s -o %(optimizer)s -i %(max_opt_steps)s --temperature %(T)s --pressure %(P)s --force_tolerance %(force_tolerance)s --ff_minimize 0 -amac 0 
    environment: python
    executable: bin/optimize_ani.py
  references:
  - input/smiles.csv:copy
  - GetMoleculeIndex:output
  - ForceFieldOptANIUncertainty/optimized.xyz:ref
  resourceManager:
    config:
      backend: '%(backend)s'
    kubernetes:
      image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/ani-torch-psi4-st4sd:0.1.0
    lsf:
      queue: normal

# Pull all of the energies together in one file over all mol
- stage: 0
  name: aggregateEnergies
  command:
    arguments: -f AniOptimize/energies.csv:ref
    environment: python
    executable: bin/aggregate_energies.py
  references:
  - AniOptimize/energies.csv:ref
  workflowAttributes:
    aggregate: true
  resourceManager:
    config:
      backend: '%(backend)s'
    kubernetes:
      image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/caf-st4sd:1.0.0
    lsf:
      queue: normal
variables:
  default:
    global:
      xyzfile: "none"
      smiles_csv: "smiles.csv"
      inchi_csv: "none"
      rep_key: "smiles"
      ani_model: "ani2x"
      optimizer: "bfgs"
      max_opt_steps: 5000
      force_tolerance: 0.05
      T: 298.15
      P: 101325.0
      numberMolecules: 1
      startIndex: 0
      defaultq: "normal"
      ff_minimize: 1
      ani_minimize_all_conformers: 0
      backend: local
  openshift:
    global:
      backend: kubernetes
      number-processors: '1'
    stages:
      0:
        stage-name: ani_optimization
version: 0.2.0
"""


@pytest.fixture(scope="function")
def output_dir() -> str:
    path = tempfile.mkdtemp()

    print('Output dir is', path)
    yield path

    try:
        shutil.rmtree(path)
    except:
        pass


def populate_files(location: str, extra_files: Dict[str, str]):
    """Populates files under some root directory
    """

    for path in extra_files:
        sub_folder, name = os.path.split(path)
        try:
            os.makedirs(os.path.join(location, sub_folder))
        except Exception as e:
            pass
        with open(os.path.join(location, path), 'w') as f:
            f.write(extra_files[path])


def package_from_files(
        location: str,
        files: Dict[str, str],
) -> str:
    package_path = os.path.join(location, str(uuid.uuid4()))
    os.makedirs(package_path)
    populate_files(package_path, files)

    return package_path


def package_from_flowir(
        flowir: str,
        location: str,
        extra_files: Dict[str, str] | None = None,
        variable_files: List[str] | None = None,
        platform: str | None = None,
        manifest_path: str | None = None,
) -> experiment.model.data.Experiment:
    package_path = os.path.join(location, '%s.package' % str(uuid.uuid4()))
    dir_conf = os.path.join(package_path, 'conf')
    os.makedirs(dir_conf)
    with open(os.path.join(dir_conf, 'flowir_package.yaml'), 'w') as f:
        f.write(flowir)

    extra_files = extra_files or {}

    populate_files(package_path, extra_files)

    if manifest_path:
        manifest_path = os.path.join(package_path, manifest_path)
    return experiment.model.storage.ExperimentPackage.packageFromLocation(
        package_path, platform=platform, manifest=manifest_path)


@pytest.fixture
def ve_toxicity_pred() -> apis.models.virtual_experiment.ParameterisedPackage:
    desc = {
        "base": {
            "packages": [
                {
                    "config": {},
                    "dependencies": {"imageRegistries": []},
                    "name": "main",
                    "source": {"git": {
                        "location": {
                            "commit": "5a9982b6a780c09f4f65f0c83064be2829df58dd",
                            "url": "https://github.ibm.com/st4sd-contrib-experiments/"
                                   "Toxicity-prediction.git"}
                    }}}]
        },
        "metadata": {"package": {
            "description": "Provides QSAR predictions of physio-chemical properties of "
                           "molecules using OPERA",
            "keywords": ["smiles", "computational chemistry", "toxicity", "opera", "QSAR"],
            "maintainer": "michaelj@ie.ibm.com", "name": "toxicity-prediction-opera", "tags": []}},
        "parameterisation": {
            "executionOptions": {
                "data": [],
                "platform": ["openshift", "sandbox"],
                "runtime": {"args": [], "resources": {}},
                "variables": [{'name': 'number-processors'}]
            },
            "presets": {"data": [], "environmentVariables": [],
                        "runtime": {"args": ["--registerWorkflow=yes"], "resources": {}},
                        "variables": []}}}

    return apis.models.virtual_experiment.ParameterisedPackage.parse_obj(desc)


@pytest.fixture
def str_toxicity_pred():
    flowir = """
    interface:
      description: "Predicts toxicity properties of small molecules"
      inputSpec:
        namingScheme: "SMILES" 
        inputExtractionMethod: 
          hookGetInputIds:
            source: 
              path: "input/input_smiles.csv"
      propertiesSpec:
        - name: "LogWS" 
          description: "Water-solubility at 25C. Units: Log10(mol/L). JRC Report ID: Q17-13-0012"
          propertyExtractionMethod:
            hookGetProperties:
              source:
                keyOutput: "ToxicityPredictionResults" 
        - name: "LogP"
          description: "Octanol-water partition coefficient. Units Log10(Unitless). JRC Report ID: Q17-16-0016"
          propertyExtractionMethod:
            hookGetProperties:
              source:
                keyOutput: "ToxicityPredictionResults" 
        - name: "Biodegradation HalfLife"
          description: "biodegradation half-life for compounds containing only carbon and hydrogen. Unit: Log10(Days). JRC Report ID: Q17-23b-0022"
          propertyExtractionMethod:
            hookGetProperties:
              source:
                keyOutput: "ToxicityPredictionResults" 
        - name: "LD50"
          description: "Collaborative Acute Toxicity Modeling Suite (CATMoS). Lethal Dose 50 point estimate model. Unit: Log10(mg/kg)"
          propertyExtractionMethod:
            hookGetProperties:
              source:
                keyOutput: "ToxicityPredictionResults" 
    status-report:
      0:
        arguments: '1'
        executable: echo
        stage-weight: 1.0
    output:
      PredictionWS:
        data-in: stage0.ToxicityPrediction/predictions_WS.csv:ref
        type: csv
      PredictionLogP:
        data-in: stage0.ToxicityPrediction/predictions_LogP.csv:ref
        type: csv
      PredictionBioDeg:
        data-in: stage0.ToxicityPrediction/predictions_BioDeg.csv:ref
        type: csv
      PredictionCATMoS:
        data-in: stage0.ToxicityPrediction/predictions_CATMoS.csv:ref
        type: csv
      RawToxicityPredictionData:
        data-in: AggregatePredictions/toxicity-predictions.csv:copy
        description: '"Raw toxicity property predictions."'
        stages:
        - 0
        type: csv
      SDFInput:
        data-in: SmilesToSDF/tox_experiment_molecules.sdf:copy
        description: '"SDF file passed in as input to opera all smiles and sdf input are
          converted to a single sdf input file in this experiment."'
        stages:
        - 0
        type: csv
      ToxicityPredictionResults:
        data-in: AggregatePropertyPredictions/toxicity-property-predictions.csv:copy
        description: '"Toxicity related property prediction results."'
        stages:
        - 0
        type: csv
    platforms:
    - default
    - hermes
    - openshift
    - sandbox
    environments:
      default:
        PYTHON: {}
      hermes:
        PYTHON: {}
      openshift:
        PYTHON: {}
      sandbox:
        PYTHON:
          PATH: $PATH
    components:
    - stage: 0
      name: SmilesToSDF
      command:
        arguments: --smiles input_smiles.csv --sdf input_sdf.sdf
        environment: python
        executable: bin/smiles_to_sdf.py
      references:
      - input/input_smiles.csv:copy
      - data/input_sdf.sdf:copy
      resourceManager:
        config:
          backend: '%(backend)s'
        kubernetes:
          image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/caf-st4sd:1.0.0
        lsf:
          dockerImage: alex.python
          dockerProfileApp: forFlow
          queue: normal
    - stage: 0
      name: ToxicityPrediction
      command:
        arguments: /usr/local/MATLAB/MATLAB_Runtime/v94 -s tox_experiment_molecules.sdf
          -o predictions.csv -a -x -n -v 2
        environment: python
        executable: /usr/local/bin/OPERA/application/run_OPERA.sh
      references:
      - SmilesToSDF/tox_experiment_molecules.sdf:copy
      resourceRequest:
        numberThreads: "%(number-processors)s"
      resourceManager:
        config:
          backend: '%(backend)s'
        kubernetes:
          image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/opera-serial-mdlab:2.6.0
        lsf:
          dockerImage: alex.python
          dockerProfileApp: forFlow
          queue: normal
          resourceString: select[hname==tuleta10]
    - stage: 0
      name: AggregatePropertyPredictions
      command:
        arguments: -i stage0.ToxicityPrediction:ref -d input/input_smiles.csv:ref -s data/input_sdf.sdf:ref
        environment: python
        executable: bin/collect_toxicity_prediction_data.py
      references:
      - stage0.ToxicityPrediction:ref
      - input/input_smiles.csv:ref
      - data/input_sdf.sdf:ref
      resourceManager:
        config:
          backend: '%(backend)s'
        kubernetes:
          image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/caf-st4sd:1.0.0
        lsf:
          dockerImage: alex.python
          dockerProfileApp: forFlow
          queue: normal
    - stage: 0
      name: AggregatePredictions
      command:
        arguments: -i stage0.ToxicityPrediction:ref
        environment: python
        executable: bin/combine_toxicity_predictions.py
      references:
      - stage0.ToxicityPrediction:ref
      resourceManager:
        config:
          backend: '%(backend)s'
        kubernetes:
          image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/caf-st4sd:1.0.0
        lsf:
          dockerImage: alex.python
          dockerProfileApp: forFlow
          queue: normal
    variables:
      default:
        global:
          number-processors: '1'
          defaultq: normal
          memory: 2Gi
          number-processors: 1
        stages:
          0:
            stage-name: Toxcitity-prediction
      hermes:
        global:
          backend: kubernetes
          number-processors: 16
      openshift:
        global:
          backend: kubernetes
      sandbox:
        global:
          backend: lsf
          defaultq: hpc-12
          number-processors: 10"""
    return flowir


@pytest.fixture
def str_sum_numbers():
    return """
variables:
  default:
    global:
      numberOfPoints: 3
      addToSum: 10
      my_random_seed: ""
      nobody_cares: this variable is not exposed to users
  
  openshift:
    global:
      addToSum: -5

blueprint:
  default:
    global:
      command:
        environment: environment

  artifactory:
    global:
      resourceRequest:
        memory: 100Mi
      resourceManager:
        config:
          backend: kubernetes
        kubernetes:
          image: res-st4sd-team-official-base-docker-local.artifactory.swg-devops.com/st4sd-runtime-core
  
  openshift:
    global:
      resourceRequest:
        memory: 100Mi
      resourceManager:
        config:
          backend: kubernetes
        kubernetes:
          image: res-st4sd-team-official-base-docker-local.artifactory.swg-devops.com/st4sd-runtime-core

  scafellpike:
    global:
      resourceManager:
        config:
          backend: lsf

    stages:
      1:
        resourceRequest:
            memory: 150Mi

environments:
  default:
    ## Defining 'default.environment' here sets the default env a job uses. If this wasn't defined, the job would use the
    ## same env as the Flow process (ie: the same as elaunch.py)
    environment:
      DEFAULTS: PATH:LD_LIBRARY_PATH
      PATH: $PATH
      LD_LIBRARY_PATH: $LD_LIBRARY_PATH

  artifactory:
    environment:
      PATH: /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

  scafellpike:
      ## When running in platform 'scafellpike', if component doesn't specify an env, it will use 'environment' defined here
      environment:
        DEFAULTS: PATH
        PATH: $PATH:/lustre/scafellpike/local/HCRI003/rla09/shared/virtualenvs/flow_2022/bin

components: 
- stage: 0
  name: GenerateInput
  command: 
    executable: "bin/generate_input_file.py"
    arguments: "%(numberOfPoints)s  %(my_random_seed)s"


- stage: 1
  name: ExtractRow
  command:
    executable: "bin/extract_row.py"
    arguments: "stage0.GenerateInput/output.csv:ref %(replica)s"
  references:
    - "stage0.GenerateInput/output.csv:ref"
  workflowAttributes:
    replicate: "%(numberOfPoints)s"

- stage: 1
  name: PartialSum
  command:
    executable: "bin/sum.py"
    arguments: "ExtractRow:output"
  references:
    - "ExtractRow:output"
  override:
    # overrides the definition of the component when the `artifactory` platform is used
    artifactory:
      command:
        arguments: "ExtractRow:output -1 8 2 3"

- stage: 2
  name: Sum
  command:
    executable: "bin/sum.py"
    arguments: "stage1.PartialSum:output %(addToSum)s"
  references:
    - "stage1.PartialSum:output"
  workflowAttributes:
    aggregate: True

- stage: 3
  name: Cat
  command:
    executable: "cat"
    arguments: "cat_me.txt"
  references:
    - "data/cat_me.txt:copy"

output:
  TotalSum:
    data-in: "stage2.Sum/out.stdout:copy"
    """


@pytest.fixture(scope="function")
def ve_sum_numbers():
    sum_numbers_def = {
        "base": {
            "packages": [{
                "source": {
                    "git": {
                        "location": {
                            "url": "https://github.ibm.com/st4sd/sum-numbers.git",
                            "branch": "main",
                        }
                    }
                },
            }]
        },
        "metadata": {
            "package": {
                "name": "http-sum-numbers",
                "tags": ["latest"],
                "maintainer": "st4sd@st4sd.st4sd"
            }
        },
        "parameterisation": {
            "presets": {
                "runtime": {
                    "args": [
                        "--failSafeDelays=no"
                    ]
                }
            },
            "executionOptions": {
                "platform": [
                    "artifactory", "default"
                ]
            }
        }
    }
    ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(sum_numbers_def)
    ve.update_digest()

    assert ve.metadata.registry.digest is not None
    return ve


@pytest.fixture(scope="function")
def sum_numbers_ve_dataset():
    sum_numbers_def = {
        "base": {
            "packages": [{
                "source": {
                    "dataset": {
                        "location": {
                            "dataset": "my-test"
                        }
                    }
                },
            }]
        },
        "metadata": {
            "package": {
                "name": "http-sum-numbers",
                "tags": ["latest"],
                "maintainer": "st4sd@st4sd.st4sd"
            }
        },
        "parameterisation": {
            "presets": {
                "runtime": {
                    "args": [
                        "--failSafeDelays=no"
                    ]
                }
            },
            "executionOptions": {
                "platform": [
                    "artifactory", "default"
                ]
            }
        }
    }
    ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(sum_numbers_def)
    ve.update_digest()

    assert ve.metadata.registry.digest is not None
    return ve


@pytest.fixture(scope="function")
def derived_ve():
    package = {
        "base": {
            "packages": [
                {
                    "name": "expensive",
                    "source": {
                        "git": {
                            "location": {
                                "url": "https://github.ibm.com/AI-reaction-modelling/psi4_optimize.git",
                                "branch": "main",
                            }
                        }
                    },
                    "graphs": [
                        {
                            "name": "prologue-epilogue",
                            "nodes": [
                                {"reference": "stage0.GetMoleculeIndex"},
                                {"reference": "stage0.ForceFieldOptANIUncertainty"},
                                {"reference": "stage0.aggregateEnergies"},
                            ],
                            "bindings": {
                                "input": [
                                    {"name": "csvSmiles", "reference": "input/smiles.csv:copy"},
                                    {
                                        "name": "simulation-energies",
                                        "reference": "stage0.Psi4Optimize/energies.csv:ref"
                                    }
                                ],
                                "output": [
                                    {
                                        "name": "aggregate-energies",
                                        "reference": "stage0.aggregateEnergies/all_energies.csv:ref"
                                    },
                                    {
                                        "name": "molecule-index",
                                        "reference": "stage0.GetMoleculeIndex:output"
                                    },
                                    {
                                        "name": "3d-geometry",
                                        "reference": "stage0.ForceFieldOptANIUncertainty/optimized.xyz:ref"
                                    }
                                ]
                            }
                        }
                    ]
                },
                {
                    "name": "surrogate",
                    "source": {
                        "git": {
                            "location": {
                                "url": "https://github.ibm.com/AI-reaction-modelling/neural_potential_optimize.git",
                                "branch": "main",
                            }
                        }
                    },
                    "graphs": [
                        {
                            "name": "simulation",
                            "nodes": [
                                {"reference": "stage0.AniOptimize"},
                            ],
                            "bindings": {
                                "input": [
                                    {"name": "csvSmiles", "reference": "input/smiles.csv:copy"},
                                    {
                                        "name": "molecule-index",
                                        "reference": "stage0.GetMoleculeIndex:output"
                                    },
                                    {
                                        "name": "3d-geometry",
                                        "reference": "stage0.ForceFieldOptANIUncertainty/optimized.xyz:ref"
                                    }
                                ],
                                "output": [
                                    {
                                        "name": "optimized-energies",
                                        "reference": "stage0.AniOptimize/energies.csv:ref"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ],
            "connections": [
                {
                    "graph": {"name": "expensive/prologue-epilogue"},
                    "bindings": [
                        {
                            "name": "csvSmiles",
                            "valueFrom": {
                                "applicationDependency": {
                                    "reference": "input/smiles.csv:ref"
                                }
                            }
                        },
                        {
                            "name": "simulation-energies",
                            "valueFrom": {
                                "graph": {
                                    "name": "surrogate/simulation",
                                    "binding": {
                                        "name": "optimized-energies"
                                    }
                                }
                            }
                        }
                    ]
                },
                {
                    "graph": {"name": "surrogate/simulation"},
                    "bindings": [
                        {
                            "name": "csvSmiles",
                            "valueFrom": {
                                "applicationDependency": {
                                    "reference": "input/smiles.csv:copy"
                                }
                            }
                        },
                        {
                            "name": "molecule-index",
                            "valueFrom": {
                                "graph": {
                                    "name": "expensive/prologue-epilogue",
                                    "binding": {
                                        "name": "molecule-index"
                                    }
                                }
                            }
                        },
                        {
                            "name": "3d-geometry",
                            "valueFrom": {
                                "graph": {
                                    "name": "expensive/prologue-epilogue",
                                    "binding": {
                                        "name": "3d-geometry"
                                    }
                                }
                            }
                        }
                    ]
                }
            ],
            "includePaths": [
                {"source": {"path": "bin/optimize_ff.py", "packageName": "expensive"}},
                {"source": {"path": "bin/aggregate_energies.py", "packageName": "expensive"}},
                {"source": {"path": "bin/optimize_ani.py", "packageName": "surrogate"}},
            ],
            "output": [
                {
                    "name": "OptimisationResults",
                    "valueFrom": {
                        "graph": {
                            # VV: FIXME: This only works if a graph is used a maximum of ONE time
                            "name": "expensive/prologue-epilogue",
                            "binding": {
                                "name": "aggregate-energies"
                            }
                        }
                    }
                }

            ],
            "interface": {
                "description": "Measures band-gap and related properties of small molecules in gas-phase using DFT",
                "inputSpec": {
                    "namingScheme": "SMILES",
                    "inputExtractionMethod": {
                        "hookGetInputIds": {
                            "source": {
                                "path": "input/pag_data.csv"
                            }
                        }
                    }
                },
                "propertiesSpec": [
                    {
                        "name": "band-gap",
                        "description": "The difference between homo and lumo in electron-volts",
                        "propertyExtractionMethod": {
                            "hookGetProperties": {
                                "source": {
                                    "keyOutput": "OptimisationResults"
                                }
                            }
                        }
                    },
                    {
                        "name": "homo",
                        "description": "The energy of the highest occuppied molecular orbital in electron-volts",
                        "propertyExtractionMethod": {
                            "hookGetProperties": {
                                "source": {
                                    "keyOutput": "OptimisationResults"
                                }
                            }
                        }
                    },
                    {
                        "name": "lumo",
                        "description": "The energy of the lowest unoccuppied molecular orbital in electron-volts",
                        "propertyExtractionMethod": {
                            "hookGetProperties": {
                                "source": {
                                    "keyOutput": "OptimisationResults"
                                }
                            }
                        }
                    },
                    {
                        "name": "electric-moments",
                        "description": "The dipole moment in debyes",
                        "propertyExtractionMethod": {
                            "hookGetProperties": {
                                "source": {
                                    "keyOutput": "OptimisationResults"
                                }
                            }
                        }
                    },
                    {
                        "name": "total-energy",
                        "description": "The total energy of the molecule in electron-volts",
                        "propertyExtractionMethod": {
                            "hookGetProperties": {
                                "source": {
                                    "keyOutput": "OptimisationResults"
                                }
                            }
                        }
                    }
                ]
            }
        },
        "metadata": {
            "package": {
                "name": "use-ani-surrogate",
                "tags": ["latest"],
                "maintainer": "st4sd@st4sd.st4sd"
            }
        },
        "parameterisation": {
            "presets": {
                "runtime": {
                    "args": [
                        "--failSafeDelays=no"
                    ]
                }
            },
            "executionOptions": {
                "platform": [
                    "openshift", "default"
                ]
            }
        }
    }
    ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(package)
    ve.update_digest()

    assert ve.metadata.registry.digest is not None
    return ve


@pytest.fixture
def mock_list_namespaced_secret(monkeypatch):
    def mock_list_namespaced_secret(self, namespace, **kwargs) -> kubernetes.client.V1SecretList:
        ret = kubernetes.client.V1SecretList(items=[])

        if (kwargs.get('field_selector', "metadata.name=my-test") == f"metadata.name=my-test" and
                namespace == MONITORED_NAMESPACE):
            ret.items.append(kubernetes.client.V1Secret(data={"oauth-token": base64.b64encode(b"my-token").decode()}))

        return ret

    monkeypatch.setattr(kubernetes.client.CoreV1Api, "list_namespaced_secret", mock_list_namespaced_secret)


@pytest.fixture
def mock_list_config_map_configuration(monkeypatch):
    def mock_list_namespaced_config_map(self, namespace, **kwargs) -> kubernetes.client.V1ConfigMapList:
        ret = kubernetes.client.V1ConfigMapList(items=[])

        if (kwargs.get('field_selector', "metadata.name=st4sd-runtime-service")
                == f"metadata.name=st4sd-runtime-service" and namespace == MONITORED_NAMESPACE):
            ret.items.append(kubernetes.client.V1ConfigMap(data={
                "config.json": """{
  "workflow-monitoring-image": 
      "res-st4sd-team-official-base-docker-local.artifactory.swg-devops.com/st4sd-runtime-k8s-monitoring:latest",
  "image": "res-st4sd-team-official-base-docker-local.artifactory.swg-devops.com/st4sd-runtime-core:latest",
  "gitsecret-oauth": "my-test",
  "imagePullSecrets": ["st4sd-base-images","st4sd-community-applications"],
  "workingVolume": "workflow-instances-pvc",
  "inputdatadir": "./examples",
  "s3-fetch-files-image": 
      "res-st4sd-team-official-base-docker-local.artifactory.swg-devops.com/st4sd-runtime-k8s-input-s3:latest",
  "default-arguments": [{"--executionMode": "production"}]
}""",
                "datastoreLabelGateway": "datastoreLabel",
                "hostDatastoreGateway": "host.st4sd.com/ds-gateway",
                "hostDatastoreRegistry": "host.st4sd.com/ds-registry",
                "hostDatastoreRest": "host.st4sd.com/ds-mongo-proxy",
                "hostRuntimeService": "host.st4sd.com/rs",
                "hostST4SD": "host.ibm.com",
            }))

        return ret

    monkeypatch.setattr(kubernetes.client.CoreV1Api, "list_namespaced_config_map", mock_list_namespaced_config_map)


@pytest.fixture
def mock_list_dataset(monkeypatch):
    def mock_list_dataset(
            self, group, version, namespace, plural, **kwargs
    ) -> Dict[str, str]:
        ret = {
            "apiVersion": "v1",
            "items": [

            ],
            "kind": "List",
            "metadata": {
                "resourceVersion": "",
                "selfLink": ""
            }
        }

        if (kwargs.get('field_selector', "metadata.name=my-test") == "metadata.name=my-test" and
                K8S_DATASET_GROUP == group and
                K8S_DATASET_VERSION == version and
                K8S_DATASET_PLURAL == plural and
                namespace == MONITORED_NAMESPACE):
            ret['items'].append({
                "apiVersion": f"{K8S_DATASET_GROUP}/{K8S_DATASET_VERSION}",
                "kind": "Dataset",
                "metadata": {
                    "creationTimestamp": "2022-08-05T13:55:45Z",
                    "generation": 1,
                    "name": "my-dataset",
                    "namespace": "vv-playground",
                    "resourceVersion": "34843808",
                    "uid": "b5ef1e29-9889-437d-a21d-9fa00e0bbcd9"
                },
                "spec": {
                    "local": {
                        "accessKeyID": "accessKeyID",
                        "bucket": "bucket",
                        "endpoint": "endpoint",
                        "region": "region",
                        "secretAccessKey": "secretAccessKey",
                        "type": "COS"
                    }
                },
                "status": {
                    "caching": {
                        "info": "No DLF caching plugins are installed",
                        "status": "Disabled"
                    },
                    "provision": {
                        "status": "OK"
                    }
                }
            })

        return ret

    monkeypatch.setattr(kubernetes.client.CustomObjectsApi, "list_namespaced_custom_object", mock_list_dataset)


@pytest.fixture()
def flowir_ani() -> str:
    return """
platforms:
- default
- openshift
blueprint:
  openshift:
    global:
      resourceManager:
        kubernetes:
          cpuUnitsPerCore: 1.0
output:
  OptimizedConfiguration:
    data-in: stage0.GenerateOptimizedConfiguration/molecule.inp:copy
    description: '"ANI optimized configuration prepared for GAMESS single-point energy calculation"'
    type: csv
components:
- stage: 0
  name: GenerateOptimizedConfiguration 
  command:
    arguments: -scsv pag_data.csv -rk %(key)s -ri %(molecule_index)s --ani_model %(ani_model)s -o %(optimizer)s --force_tolerance %(force_tolerance)s --ff_minimize %(ff_minimize)s -amac %(ani_minimize_all_conformers)s --test -og input_molecule.txt --n_conformers %(n_conformers)s --max_iterations %(max_iterations)s
    environment: None
    executable: bin/optimize_ani.py
  references:
  - input/pag_data.csv:copy
  - input/input_molecule.txt:copy
  resourceManager:
    config:
      backend: '%(backend)s'
    kubernetes:
      image: res-st4sd-community-team-applications-docker-local.artifactory.swg-devops.com/ani-torch-st4sd:2.2.2
variables:
  default:
    global:
      molecule_index: 0
      cpuUnitsPerCore: '1'
      defaultq: normal
      functional: "wB97X"
      ani_model: "ani2x"
      optimizer: "bfgs"
      force_tolerance: 0.05
      ff_minimize: 0
      ani_minimize_all_conformers: 1
      key: "smiles"
      n_conformers: 50
      max_iterations: 5000
  openshift:
    global:
      backend: kubernetes
      gamess-version-number: '01'
      number-processors: '1'
"""


@pytest.fixture()
def flowir_gamess_homo_lumo_dft() -> str:
    return """
interface:
  description: "Measures band-gap and related properties of small molecules in gas-phase using DFT"
  inputSpec:
    namingScheme: "SMILES"
    inputExtractionMethod:
      hookGetInputIds:
        source:
          path: "input/pag_data.csv"
  propertiesSpec:
    - name: "band-gap"
      description: "The difference between homo and lumo in electron-volts"
      propertyExtractionMethod:
        hookGetProperties:
          source:
            keyOutput: "OptimisationResults"
    - name: "homo"
      description: "The energy of the highest occuppied molecular orbital in electron-volts"
      propertyExtractionMethod:
        hookGetProperties:
          source:
            keyOutput: "OptimisationResults"
    - name: "lumo"
      description: "The energy of the lowest unoccuppied molecular orbital in electron-volts"
      propertyExtractionMethod:
        hookGetProperties:
          source:
            keyOutput: "OptimisationResults"
    - name: "electric-moments"
      description: "The dipole moment in debyes"
      propertyExtractionMethod:
        hookGetProperties:
          source:
            keyOutput: "OptimisationResults"
    - name: "total-energy"
      description: "The total energy of the molecule in electron-volts"
      propertyExtractionMethod:
        hookGetProperties:
          source:
            keyOutput: "OptimisationResults"
status-report:
  0:
    arguments: '1'
    executable: echo
    stage-weight: 0.1
  1:
    arguments: '1'
    executable: echo
    stage-weight: 0.9
output:
  OptimisationResults:
    data-in: stage1.ExtractEnergies/energies.csv:ref
    description: homo/lumo results
    type: csv

platforms:
- default
- hermes
- openshift
- openshift-kubeflux
- sandbox

blueprint:
  openshift-kubeflux:
    global:
      resourceManager:
        kubernetes:
          podSpec:
            schedulerName: kubeflux

environments:
  # Platforms that do not override the environments, use the ones that default definess
  default:
    GAMESS:
      GMSPATH: /gamess/
      PATH: /gamess/:$PATH
    PYTHON: {}
  sandbox:
    GAMESS:
      PATH: $PATH:/gpfs/users/axh44-sxa03/Projects/FoC-AI-Simulator/gamess:/gamess:/usr/bin:/bin
    PYTHON:
      PATH: $PATH

components:
- stage: 0
  name: SetBasis
  command:
    arguments: sed -i'.bak' -e 's/#BASIS#/%(basis)s/g' input_molecule.txt
    interpreter: bash
  references:
  - data/input_molecule.txt:copy
- stage: 0
  name: SetFunctional
  command:
    arguments: sed -i'.bak' -e 's/#FUNCTIONAL#/%(functional)s/g' input_molecule.txt
    interpreter: bash
  references:
  - SetBasis/input_molecule.txt:copy
- stage: 0
  name: GetMoleculeIndex
  command:
    arguments: -c \"print(%(startIndex)s + %(replica)s),\"
    executable: python
  workflowAttributes:
    replicate: '%(numberMolecules)s'
- stage: 0
  name: AnionSMILESToGAMESSInput
  command:
    arguments: --input pag_data.csv --template input_molecule.txt --row GetMoleculeIndex:output
    environment: python
    executable: bin/rdkit_smiles2coordinates.py
  references:
  - input/pag_data.csv:copy
  - SetFunctional/input_molecule.txt:copy
  - GetMoleculeIndex:output
  resourceManager:
    config:
      backend: '%(backend)s'
    kubernetes:
      image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/caf-st4sd:1.0.0
    lsf:
      dockerImage: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/caf-st4sd:1.0.0
      dockerProfileApp: st4sd
      queue: normal
      resourceString: select[hname==tuleta10]
- stage: 1
  name: GeometryOptimisation
  command:
    arguments: molecule.inp %(gamess-version-number)s %(number-processors)s
    environment: gamess
    executable: rungms
  references:
  - stage0.AnionSMILESToGAMESSInput/molecule.inp:copy
  workflowAttributes:
    restartHookFile: "%(gamess-restart-hook-file)s"
    restartHookOn:
    - KnownIssue
    - Success
    - ResourceExhausted
    shutdownOn:
    - KnownIssue
  resourceManager:
    config:
      backend: '%(backend)s'
      walltime: 700.0
    kubernetes:
      gracePeriod: 1800
      image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/gamess-st4sd:2019.11.30
    lsf:
      dockerImage: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/gamess-st4sd:2019.11.30
      dockerProfileApp: st4sd
      queue: normal
      resourceString: select[hname==tuleta10]
  resourceRequest:
    memory: '%(mem)s'
    numberThreads: '%(number-processors)s'
    threadsPerCore: 1
- stage: 1
  name: AnalyzeEnergies
  command:
    arguments: -f GeometryOptimisation:ref/out.stdout*
    environment: python
    executable: bin/features_and_convergence.py
  references:
  - GeometryOptimisation:ref
  workflowAttributes:
    shutdownOn:
    - KnownIssue
  resourceManager:
    config:
      backend: '%(backend)s'
    kubernetes:
      image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/caf-st4sd:1.0.0
    lsf:
      queue: normal
      resourceString: select[hname==tuleta10]
- stage: 1
  name: CreateLabels
  command:
    arguments: -c \"import utilities.data; m=utilities.data.matrixFromCSVFile('input/pag_data.csv:ref');
      print(','.join([str(m.getElements(int(i))['label']) for i in '''stage0.GetMoleculeIndex:output'''.split()]))\"
    executable: python
  references:
  - stage0.GetMoleculeIndex:output
  - input/pag_data.csv:ref
  workflowAttributes:
    aggregate: true
- stage: 1
  name: ExtractEnergies
  command:
    arguments: -l CreateLabels:output GeometryOptimisation:ref
    environment: python
    executable: bin/extract_gmsout.py
  references:
  - GeometryOptimisation:ref
  - CreateLabels:output
  workflowAttributes:
    aggregate: true
  resourceManager:
    config:
      backend: '%(backend)s'
    kubernetes:
      image: res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/pyopenbabel-st4sd:3.1.1
    lsf:
      queue: normal
      resourceString: select[hname==tuleta10]
variables:
  default:
    global:
      # VV: References python script in hooks directory to use for restartHook of GeometryOptimisation
      gamess-restart-hook-file: dft_restart.py
      defaultq: normal
      mem: '4295000000'
      backend: local
      number-processors: '1'
      startIndex: '0'
      numberMolecules: '1'
      gamess-version-number: '01'
      basis: GBASIS=PM3
      functional: B3LYP
    stages:
      0:
        stage-name: SMILES_to_GAMESS
      1:
        stage-name: GeometryOptimisationRun
  hermes:
    global:
      backend: kubernetes
      number-processors: '16'
  openshift:
    global:
      backend: kubernetes
  openshift-kubeflux:
    global:
      backend: kubernetes
  sandbox:
    global:
      backend: lsf
      defaultq: normal
      number-processors: '10'
"""


@pytest.fixture(scope="function")
def derived_ve_gamess_homo_dft_ani():
    package = definition = {
        "metadata": {
            "package": {
                "name": "homolumo-ani-surrogate",
                "tags": [
                    "latest"
                ],
                "maintainer": "st4sd@st4sd.st4sd"
            }
        },
        "base": {
            "packages": [
                {
                    "name": "homo-lumo-dft-gamess-us:latest",
                    "source": {
                        "git": {
                            "location": {
                                "url": "https://github.ibm.com/st4sd-contrib-experiments/"
                                       "homo-lumo-dft.git",
                                "branch": "master"
                            }
                        }
                    },
                    "config": {
                        "path": "dft/homo-lumo-dft.yaml",
                        "manifestPath": "dft/manifest.yaml"
                    },
                    "graphs": [
                        {
                            "name": "prologue-epilogue",
                            "nodes": [
                                {"reference": "stage0.SetBasis"},
                                {"reference": "stage0.SetFunctional"},
                                {"reference": "stage0.GetMoleculeIndex"},
                                {"reference": "stage1.GeometryOptimisation"},
                                {"reference": "stage1.AnalyzeEnergies"},
                                {"reference": "stage1.CreateLabels"},
                                {"reference": "stage1.ExtractEnergies"}
                            ],
                            "bindings": {
                                "input": [
                                    {
                                        "name": "input-molecule",
                                        "reference": "stage0.AnionSMILESToGAMESSInput/"
                                                     "molecule.inp:copy"
                                    }
                                ],
                                "output": [
                                    {
                                        "name": "optimisation-results",
                                        "reference": "stage1.ExtractEnergies/energies.csv:ref"
                                    },
                                    {
                                        "name": "functional",
                                        "reference": "stage0.SetFunctional/input_molecule.txt:copy"
                                    },
                                    {
                                        "name": "aggregate-energies",
                                        "reference": "stage1.ExtractEnergies/energies.csv:ref"
                                    }
                                ]
                            }
                        }
                    ]
                },
                {
                    "name": "configuration-generator-ani-gamess:latest",
                    "source": {
                        "git": {
                            "location": {
                                "url": "https://github.ibm.com/st4sd-contrib-experiments/"
                                       "gamess-input-ani.git",
                                "branch": "master",
                            }
                        }
                    },
                    "config": {
                        "path": "ani-surrogate.yaml",
                        "manifestPath": "manifest.yaml"
                    },
                    "graphs": [
                        {
                            "name": "optimized-molecule",
                            "nodes": [
                                {
                                    "reference": "stage0.GenerateOptimizedConfiguration"
                                },
                            ],
                            "bindings": {
                                "input": [
                                    {
                                        "name": "pag-data",
                                        "reference": "input/pag_data.csv:copy"
                                    },
                                    {
                                        "name": "input-molecule",
                                        "reference": "input/input_molecule.txt:copy"
                                    }
                                ],
                                "output": [
                                    {
                                        "name": "optimized-molecule",
                                        "reference": "stage0.GenerateOptimizedConfiguration/"
                                                     "molecule.inp:copy"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ],

            "connections": [
                {
                    "graph": {
                        "name": "homo-lumo-dft-gamess-us:latest/prologue-epilogue"
                    },
                    "bindings": [
                        {
                            # VV: This is the inputBinding of homo-lumo-dft-gamess-us/prologue-epilogue
                            "name": "input-molecule",
                            "valueFrom": {
                                "graph": {
                                    # format is $basePackageName/$graphName
                                    "name": "configuration-generator-ani-gamess:latest/"
                                            "optimized-molecule",
                                    "binding": {
                                        # VV: This is the outputBinding of
                                        # configuration-generator-ani-gamess/initial-molecule
                                        "name": "optimized-molecule"
                                    }
                                }
                            }
                        }
                    ]
                },
                {
                    "graph": {
                        "name": "configuration-generator-ani-gamess:latest/optimized-molecule"
                    },
                    "bindings": [
                        {
                            "name": "input-molecule",
                            "valueFrom": {
                                "graph": {
                                    "name": "homo-lumo-dft-gamess-us:latest/prologue-epilogue",
                                    "binding": {
                                        "name": "functional"
                                    }
                                }
                            }
                        }
                    ]
                }
            ],
            "includePaths": [
                {
                    "source": {
                        "path": "../component-scripts/features_and_convergence.py",
                        "packageName": "homo-lumo-dft-gamess-us:latest"
                    },
                    "dest": {
                        "path": "bin/features_and_convergence.py"
                    }
                },
                {
                    "source": {
                        "path": "../component-scripts/extract_gmsout.py",
                        "packageName": "homo-lumo-dft-gamess-us:latest"
                    },
                    "dest": {
                        "path": "bin/extract_gmsout.py"
                    }
                },
                {
                    "source": {
                        "path": "bin/optimize_ani.py",
                        "packageName": "configuration-generator-ani-gamess:latest"
                    }
                },
                {
                    "source": {
                        "path": "../hooks",
                        "packageName": "homo-lumo-dft-gamess-us:latest"
                    }
                },

                # VV: data files go here
                {
                    "source": {
                        "path": "data-dft/input_molecule.txt",
                        "packageName": "homo-lumo-dft-gamess-us:latest"
                    },
                    "dest": {
                        "path": "data/input_molecule.txt"
                    }
                },
            ],
            "output": [
                {
                    "name": "OptimisationResults",
                    "valueFrom": {
                        "graph": {
                            "name": "homo-lumo-dft-gamess-us:latest/prologue-epilogue",
                            "binding": {
                                "name": "aggregate-energies"
                            }
                        }
                    }
                }
            ],
            "interface": {
                "description": "Measures band-gap and related properties of small "
                               "molecules in gas-phase using DFT",
                "inputSpec": {
                    "namingScheme": "SMILES",
                    "inputExtractionMethod": {
                        "hookGetInputIds": {
                            "source": {
                                "path": "input/pag_data.csv"
                            }
                        }
                    }
                },
                "propertiesSpec": [
                    {
                        "name": "band-gap",
                        "description": "The difference between homo and lumo in electron-volts",
                        "propertyExtractionMethod": {
                            "hookGetProperties": {
                                "source": {
                                    "keyOutput": "OptimisationResults"
                                }
                            }
                        }
                    },
                    {
                        "name": "homo",
                        "description": "The energy of the highest occuppied molecular orbital in "
                                       "electron-volts",
                        "propertyExtractionMethod": {
                            "hookGetProperties": {
                                "source": {
                                    "keyOutput": "OptimisationResults"
                                }
                            }
                        }
                    },
                    {
                        "name": "lumo",
                        "description": "The energy of the lowest unoccuppied molecular orbital in "
                                       "electron-volts",
                        "propertyExtractionMethod": {
                            "hookGetProperties": {
                                "source": {
                                    "keyOutput": "OptimisationResults"
                                }
                            }
                        }
                    },
                    {
                        "name": "electric-moments",
                        "description": "The dipole moment in debyes",
                        "propertyExtractionMethod": {
                            "hookGetProperties": {
                                "source": {
                                    "keyOutput": "OptimisationResults"
                                }
                            }
                        }
                    },
                    {
                        "name": "total-energy",
                        "description": "The total energy of the molecule in electron-volts",
                        "propertyExtractionMethod": {
                            "hookGetProperties": {
                                "source": {
                                    "keyOutput": "OptimisationResults"
                                }
                            }
                        }
                    }
                ]
            }
        },
        "parameterisation": {
            "presets": {
                "runtime": {
                    "args": [
                        "--failSafeDelays=no",
                        "--registerWorkflow=yes"
                    ]
                }
            },
            "executionOptions": {
                "platform": [
                    "openshift"
                ],
                "variables": [
                    {
                        "name": "numberMolecules"
                    }
                ]
            }
        }
    }
    ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(package)
    ve.update_digest()

    assert ve.metadata.registry.digest is not None
    return ve


@pytest.fixture(scope="function")
def ve_homo_lumo_dft_gamess_us():
    package = {
        "base": {
            "packages": [
                {
                    "name": "main",
                    "source": {
                        "git": {
                            "location": {
                                "url": "https://github.ibm.com/st4sd-contrib-experiments/"
                                       "homo-lumo-dft.git",
                                "branch": "master"
                            },
                            "version": "0f1dc76256d30e8343fbb43bda407d05a295c687"
                        }
                    },
                    "dependencies": {
                        "imageRegistries": []
                    },
                    "config": {
                        "path": "dft/homo-lumo-dft.yaml",
                        "manifestPath": "dft/manifest.yaml"
                    },
                    "graphs": []
                }
            ],
            "connections": [],
            "includePaths": [],
            "output": []
        },
        "metadata": {
            "package": {
                "name": "homo-lumo-dft-gamess-us",
                "tags": [
                    "latest"
                ],
                "keywords": [
                    "smiles",
                    "computational chemistry",
                    "homo-lumo",
                    "semi-empirical",
                    "kubeflux",
                    "lsf"
                ],
                "maintainer": "michaelj@ie.ibm.com",
                "description": "Uses the DFT functional and basis set B3LYP/6-31G(d,p) with Grimme et al's D3 correction to perform geometry optimization and HOMO-LUMO band gap calculation"
            },
            "registry": {
                "createdOn": "2022-10-05T18:02:22.898903+0000",
                "digest": "sha256xd30e55f35cc3fd66feac34f18c114eba2e009a1b386be78df9bd125f",
                "tags": [
                    "latest"
                ],
                "timesExecuted": 1,
                "interface": {
                    "description": "Measures band-gap and related properties of small molecules in gas-phase using DFT",
                    "inputSpec": {
                        "namingScheme": "SMILES",
                        "inputExtractionMethod": {
                            "hookGetInputIds": {
                                "source": {
                                    "path": "input/pag_data.csv"
                                }
                            }
                        },
                        "hasAdditionalData": False
                    },
                    "propertiesSpec": [
                        {
                            "name": "band-gap",
                            "description": "The difference between homo and lumo in electron-volts",
                            "propertyExtractionMethod": {
                                "hookGetProperties": {
                                    "source": {
                                        "keyOutput": "OptimisationResults"
                                    }
                                }
                            }
                        },
                        {
                            "name": "homo",
                            "description": "The energy of the highest occuppied molecular orbital in electron-volts",
                            "propertyExtractionMethod": {
                                "hookGetProperties": {
                                    "source": {
                                        "keyOutput": "OptimisationResults"
                                    }
                                }
                            }
                        },
                        {
                            "name": "lumo",
                            "description": "The energy of the lowest unoccuppied molecular orbital in electron-volts",
                            "propertyExtractionMethod": {
                                "hookGetProperties": {
                                    "source": {
                                        "keyOutput": "OptimisationResults"
                                    }
                                }
                            }
                        },
                        {
                            "name": "electric-moments",
                            "description": "The dipole moment in debyes",
                            "propertyExtractionMethod": {
                                "hookGetProperties": {
                                    "source": {
                                        "keyOutput": "OptimisationResults"
                                    }
                                }
                            }
                        },
                        {
                            "name": "total-energy",
                            "description": "The total energy of the molecule in electron-volts",
                            "propertyExtractionMethod": {
                                "hookGetProperties": {
                                    "source": {
                                        "keyOutput": "OptimisationResults"
                                    }
                                }
                            }
                        }
                    ],
                    "additionalInputData": None,
                    "inputs": None,
                    "outputFiles": []
                },
                "inputs": [
                    {
                        "name": "pag_data.csv"
                    }
                ],
                "data": [
                    {
                        "name": "input_cation.txt"
                    },
                    {
                        "name": "input_anion.txt"
                    },
                    {
                        "name": "input_molecule.txt"
                    },
                    {
                        "name": "input_neutral.txt"
                    }
                ],
                "containerImages": [
                    {
                        "name": "res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/caf-st4sd:1.0.0"
                    },
                    {
                        "name": "res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/gamess-st4sd:2019.11.30"
                    },
                    {
                        "name": "res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/pyopenbabel-st4sd:3.1.1"
                    }
                ],
                "executionOptionsDefaults": {
                    "variables": [
                        {
                            "name": "numberMolecules",
                            "valueFrom": [
                                {
                                    "value": "1",
                                    "platform": "openshift"
                                },
                                {
                                    "value": "1",
                                    "platform": "openshift-kubeflux"
                                },
                                {
                                    "value": "1",
                                    "platform": "hermes"
                                },
                                {
                                    "value": "1",
                                    "platform": "sandbox"
                                }
                            ]
                        },
                        {
                            "name": "startIndex",
                            "valueFrom": [
                                {
                                    "value": "0",
                                    "platform": "openshift"
                                },
                                {
                                    "value": "0",
                                    "platform": "openshift-kubeflux"
                                },
                                {
                                    "value": "0",
                                    "platform": "hermes"
                                },
                                {
                                    "value": "0",
                                    "platform": "sandbox"
                                }
                            ]
                        },
                        {
                            "name": "mem",
                            "valueFrom": [
                                {
                                    "value": "4295000000",
                                    "platform": "openshift"
                                },
                                {
                                    "value": "4295000000",
                                    "platform": "openshift-kubeflux"
                                },
                                {
                                    "value": "4295000000",
                                    "platform": "hermes"
                                },
                                {
                                    "value": "4295000000",
                                    "platform": "sandbox"
                                }
                            ]
                        },
                        {
                            "name": "functional",
                            "valueFrom": [
                                {
                                    "value": "B3LYP",
                                    "platform": "openshift"
                                },
                                {
                                    "value": "B3LYP",
                                    "platform": "openshift-kubeflux"
                                },
                                {
                                    "value": "B3LYP",
                                    "platform": "hermes"
                                },
                                {
                                    "value": "B3LYP",
                                    "platform": "sandbox"
                                }
                            ]
                        },
                        {
                            "name": "basis",
                            "valueFrom": [
                                {
                                    "value": "GBASIS=N31 NGAUSS=6 NDFUNC=2 NPFUNC=1 DIFFSP=.TRUE. DIFFS=.TRUE.",
                                    "platform": "openshift"
                                },
                                {
                                    "value": "GBASIS=N31 NGAUSS=6 NDFUNC=2 NPFUNC=1 DIFFSP=.TRUE. DIFFS=.TRUE.",
                                    "platform": "openshift-kubeflux"
                                },
                                {
                                    "value": "GBASIS=N31 NGAUSS=6 NDFUNC=2 NPFUNC=1 DIFFSP=.TRUE. DIFFS=.TRUE.",
                                    "platform": "hermes"
                                },
                                {
                                    "value": "GBASIS=N31 NGAUSS=6 NDFUNC=2 NPFUNC=1 DIFFSP=.TRUE. DIFFS=.TRUE.",
                                    "platform": "sandbox"
                                }
                            ]
                        }
                    ]
                }
            }
        },
        "parameterisation": {
            "presets": {
                "variables": [
                    {
                        "name": "functional",
                        "value": "B3LYP"
                    },
                    {
                        "name": "basis",
                        "value": "GBASIS=N31 NGAUSS=6 NDFUNC=2 NPFUNC=1 DIFFSP=.TRUE. DIFFS=.TRUE."
                    }
                ],
                "runtime": {
                    "resources": {},
                    "args": [
                        "--failSafeDelays=no",
                        "--registerWorkflow=yes"
                    ]
                },
                "data": [],
                "environmentVariables": []
            },
            "executionOptions": {
                "variables": [
                    {
                        "name": "numberMolecules"
                    },
                    {
                        "name": "startIndex"
                    },
                    {
                        "name": "mem"
                    }
                ],
                "data": [],
                "runtime": {
                    "resources": {},
                    "args": []
                },
                "platform": [
                    "openshift",
                    "openshift-kubeflux",
                    "hermes",
                    "sandbox"
                ]
            }
        }
    }
    ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(package)
    ve.update_digest()

    assert ve.metadata.registry.digest is not None
    return ve


@pytest.fixture(scope="function")
def ve_psi4():
    package = {
        "base": {
            "packages": [
                {
                    "name": "main",
                    "source": {
                        "git": {
                            "location": {
                                "url": "https://github.ibm.com/st4sd-contrib-experiments/psi4_optimize.git",
                                "branch": "master"
                            }
                        }
                    }
                }
            ]
        },
        "metadata": {
            "package": {
                "name": "psi4",
            },
        },
        "parameterisation": {
            "presets": {
                "platform": "openshift"
            }
        }
    }
    ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(package)
    ve.update_digest()

    assert ve.metadata.registry.digest is not None
    return ve


@pytest.fixture(scope="function")
def ve_neural_potential():
    package = {
        "base": {
            "packages": [
                {
                    "name": "main",
                    "source": {
                        "git": {
                            "location": {
                                "url": "https://github.ibm.com/Vassilis-Vassiliadis/neural_potential_optimize.git",
                                "branch": "master"
                            }
                        }
                    }
                }
            ]
        },
        "metadata": {
            "package": {
                "name": "neural-potential"
            }
        },
        "parameterisation": {
            "presets": {
                "platform": "openshift"
            }
        }
    }
    ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(package)
    ve.update_digest()

    assert ve.metadata.registry.digest is not None
    return ve


@pytest.fixture(scope="function")
def ve_configuration_generator_ani() -> apis.models.virtual_experiment.ParameterisedPackage:
    package = {
        "base": {
            "packages": [
                {
                    "config": {
                        "manifestPath": "manifest.yaml",
                        "path": "ani-surrogate.yaml"
                    },
                    "dependencies": {
                        "imageRegistries": []
                    },
                    "name": "main",
                    "source": {
                        "git": {
                            "location": {
                                "branch": "master",
                                "url": "https://github.ibm.com/st4sd-contrib-experiments/gamess-input-ani.git"
                            },
                            "version": "cf2ba172242a0676ae3bddb57059c0aca5e08bf3"
                        }
                    }
                }
            ]
        },
        "metadata": {
            "package": {
                "description": "Surrogate that optimizes the geometry of a molecule using the ANI neural potential (ani2x, functional: vWB97x) and adds it to a GAMESS molecule.inp file",
                "keywords": [
                    "smiles",
                    "computational chemistry",
                    "geometry-optimization",
                    "gamess",
                    "surrogate"
                ],
                "maintainer": "michaelj@ie.ibm.com",
                "name": "configuration-generator-ani-gamess",
                "tags": [
                    "1.0"
                ]
            },
            "registry": {
                "containerImages": [
                    {
                        "name": "res-st4sd-community-team-applications-docker-local.artifactory.swg-devops.com/ani-torch-st4sd:2.2.2"
                    }
                ],
                "createdOn": "2022-09-24T16:13:59.035336+0000",
                "data": [],
                "digest": "sha256x3a19ffc0b0ecd301486fdf605279d8f952b809f37b69272de379689d",
                "executionOptionsDefaults": {
                    "variables": [
                        {
                            "name": "molecule_index",
                            "valueFrom": [
                                {
                                    "platform": "openshift",
                                    "value": "0"
                                }
                            ]
                        },
                        {
                            "name": "n_conformers",
                            "valueFrom": [
                                {
                                    "platform": "openshift",
                                    "value": "50"
                                }
                            ]
                        },
                        {
                            "name": "max_iterations",
                            "valueFrom": [
                                {
                                    "platform": "openshift",
                                    "value": "5000"
                                }
                            ]
                        },
                        {
                            "name": "ani_model",
                            "valueFrom": [
                                {
                                    "platform": "openshift",
                                    "value": "ani2x"
                                }
                            ]
                        },
                        {
                            "name": "optimizer",
                            "valueFrom": [
                                {
                                    "platform": "openshift",
                                    "value": "bfgs"
                                }
                            ]
                        },
                        {
                            "name": "functional",
                            "valueFrom": [
                                {
                                    "platform": "openshift",
                                    "value": "wB97X"
                                }
                            ]
                        }
                    ]
                },
                "inputs": [
                    {
                        "name": "pag_data.csv"
                    },
                    {
                        "name": "input_molecule.txt"
                    }
                ],
                "interface": {},
                "tags": [
                    "1.0",
                    "latest"
                ],
                "timesExecuted": 0
            }
        },
        "parameterisation": {
            "executionOptions": {
                "data": [],
                "platform": [],
                "runtime": {
                    "args": [],
                    "resources": {}
                },
                "variables": [
                    {
                        "name": "molecule_index"
                    },
                    {
                        "name": "n_conformers"
                    },
                    {
                        "name": "max_iterations"
                    }
                ]
            },
            "presets": {
                "data": [],
                "environmentVariables": [],
                "platform": "openshift",
                "runtime": {
                    "args": [
                        "--registerWorkflow=yes"
                    ],
                    "resources": {}
                },
                "variables": [
                    {
                        "name": "ani_model",
                        "value": "ani2x"
                    },
                    {
                        "name": "optimizer",
                        "value": "bfgs"
                    },
                    {
                        "name": "functional",
                        "value": "wB97X"
                    }
                ]
            }
        }
    }
    ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(package)
    ve.update_digest()

    assert ve.metadata.registry.digest is not None
    return ve


@pytest.fixture()
def homolumogamess_ani_package_metadata(
        flowir_gamess_homo_lumo_dft: str,
        flowir_ani: str,
        output_dir: str,
) -> apis.storage.PackageMetadataCollection:
    expensive_location = package_from_files(
        location=os.path.join(output_dir, "homo-lumo-dft-gamess-us"),
        files={
            'bin/aggregate_energies.py': 'expensive',
            'dft/data-dft/input_anion.txt': 'expensive',
            'dft/data-dft/input_cation.txt': 'expensive',
            'dft/data-dft/input_molecule.txt': 'expensive',
            'dft/data-dft/input_neutral.txt': 'expensive',
            'component-scripts/csv2inp.py': "expensive",
            'component-scripts/features_and_convergence.py': "expensive",
            'component-scripts/rdkit_smiles2coordinates.py': "expensive",
            'component-scripts/featurize_gamess.py': "expensive",
            'component-scripts/extract_gmsout.py': "expensive",
            'hooks/__init__.py': "",
            'hooks/dft_restart.py': "expensive",
            'hooks/interface.py': "expensive",
            'hooks/semi_empirical_restart.py': "expensive",

            'dft/homo-lumo-dft.yaml': flowir_gamess_homo_lumo_dft,
            'dft/manifest.yaml': """
                bin: ../component-scripts:copy
                data: data-dft:copy
                hooks: ../hooks:copy
                """,
        }
    )

    surrogate_location = package_from_files(
        location=os.path.join(output_dir, "configuration-generator-ani-gamess"),
        files={
            'bin/optimize_ani.py': "surrogate",
            "manifest.yaml": """
                bin: bin:copy
                """,
            "ani-surrogate.yaml": flowir_ani,
        }
    )

    StorageMetadata = apis.models.virtual_experiment.StorageMetadata

    packages_metadata = apis.storage.PackageMetadataCollection({
        'homo-lumo-dft-gamess-us:latest': StorageMetadata.from_config(
            apis.models.virtual_experiment.BasePackageConfig(
                path=os.path.join("dft", "homo-lumo-dft.yaml"), manifestPath=os.path.join("dft", "manifest.yaml")),
            prefix_paths=expensive_location
        ),
        'configuration-generator-ani-gamess:latest': StorageMetadata.from_config(
            apis.models.virtual_experiment.BasePackageConfig(
                path="ani-surrogate.yaml", manifestPath="manifest.yaml"), prefix_paths=surrogate_location
        )
    })

    return packages_metadata


@pytest.fixture()
def package_metadata_psi4_neural_potential(
        flowir_psi4: str,
        flowir_neural_potential: str,
        output_dir: str,
) -> apis.storage.PackageMetadataCollection:
    expensive_location = package_from_files(
        location=os.path.join(output_dir, "psi4"),
        files={
            'bin/aggregate_energies.py': 'expensive',
            'bin/optimize_ff.py': 'expensive',
            'bin/optimize_psi4.py': 'expensive',

            'conf/flowir_package.yaml': flowir_psi4,
        }
    )

    surrogate_location = package_from_files(
        location=os.path.join(output_dir, "neural-potential"),
        files={
            'bin/aggregate_energies.py': 'expensive',
            'bin/optimize_ff.py': 'expensive',
            'bin/optimize_ani.py': 'expensive',

            'conf/flowir_package.yaml': flowir_neural_potential,
        }
    )

    StorageMetadata = apis.models.virtual_experiment.StorageMetadata

    packages_metadata = apis.storage.PackageMetadataCollection({
        'psi4:latest': StorageMetadata.from_config(
            prefix_paths=expensive_location, config=apis.models.virtual_experiment.BasePackageConfig(),
        ),
        'neural-potential:latest': StorageMetadata.from_config(
            prefix_paths=surrogate_location, config=apis.models.virtual_experiment.BasePackageConfig(),
        )
    })

    return packages_metadata
