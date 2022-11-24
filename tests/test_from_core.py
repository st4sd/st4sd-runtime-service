# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import apis.models.from_core


def test_datarefernce_pathref():
    d1 = apis.models.from_core.DataReference("input/pag_data.csv:copy")
    d2 = apis.models.from_core.DataReference("input/input_molecule.txt:copy")

    assert d1.pathRef == "pag_data.csv"
    assert d2.pathRef == "input_molecule.txt"
