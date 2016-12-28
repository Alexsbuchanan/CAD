#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Contextual Anomaly Detector — Open Source Edition
#
# Copyright © 2016 Gregory Petrosyan <gregory.petrosyan@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, version 3 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# -----------------------------------------------------------------------------

import datetime
import math
import sys

from cad_ose import ContextualAnomalyDetector


def main():
    nrows = int(sys.argv[1])
    min_ = float(sys.argv[2])
    max_ = float(sys.argv[3])

    base_threshold = 0.75
    max_lsemi_ctxs_length = 7
    max_active_neurons_num = 15
    num_norm_value_bits = 3

    run(nrows=nrows,
        min_=min_,
        max_=max_,
        base_threshold=base_threshold,
        max_lsemi_ctxs_length=max_lsemi_ctxs_length,
        max_active_neurons_num=max_active_neurons_num,
        num_norm_value_bits=num_norm_value_bits)


def run(nrows, min_, max_, base_threshold, max_lsemi_ctxs_length, max_active_neurons_num, num_norm_value_bits):
    learning_period = min(math.floor(0.15 * nrows), 0.15 * 5000)

    cad = ContextualAnomalyDetector(
        min_value=min_,
        max_value=max_,
        base_threshold=base_threshold,
        rest_period=learning_period / 5.0,
        max_lsemi_ctxs_length=max_lsemi_ctxs_length,
        max_active_neurons_num=max_active_neurons_num,
        num_norm_value_bits=num_norm_value_bits
    )

    sys.stdin.readline()

    for line in sys.stdin:
        row = line.split(',')

        input_data = {
            'timestamp': datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S'),
            'value':     float(row[1]),
        }

        score = cad.get_anomaly_score(input_data)

        print '{},{},{}'.format(row[0].strip(), row[1].strip(), score)


if __name__ == '__main__':
    main()
