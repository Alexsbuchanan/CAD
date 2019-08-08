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

import pandas as pd
import numpy as np

from cadose.cad_ose import ContextualAnomalyDetector


def main():
    base_threshold = 0.75
    rest_period = 1
    max_lsemi_ctxs_len = 7
    max_active_neurons_num = 15
    num_norm_value_bits = 10

    run(nrows=2500,
        min_=0,
        max_=100,
        base_threshold=base_threshold,
        max_lsemi_ctxs_len=max_lsemi_ctxs_len,
        max_active_neurons_num=max_active_neurons_num,
        num_norm_value_bits=num_norm_value_bits,
        rest_period=rest_period)


def run(nrows, min_, max_,
        base_threshold,
        max_lsemi_ctxs_len,
        max_active_neurons_num,
        num_norm_value_bits,
        rest_period):

    # how much we want to learn before we care about good scores
    # learning_period = min(math.floor(0.15 * nrows), 0.15 * 5000)
    # rest_period = learning_period/5.0
    cad = ContextualAnomalyDetector(
        min_value=min_,  # Minimum value allowed by the system
        max_value=max_,  # Maximum value allowed by the system
        base_threshold=base_threshold,  # Base threshold to alert on
        rest_period=rest_period,  # Duration we wait before next alert
        max_lsemi_ctxs_len=max_lsemi_ctxs_len,
        max_active_neurons_num=max_active_neurons_num,
        num_norm_value_bits=num_norm_value_bits
    )

    with open('anomalydata/dummy2.csv') as file:
        data = pd.read_csv(file)

    score = []
    values = data['value'].values
    print(max(values), bin(int(max(values))), len(bin(int(max(values)))))
    print(min(values), bin(int(min(values))), len(bin(int(min(values)))))
    for line in values:
        score.append(cad.get_anomaly_score(line))

    print(cad.get_avg_time())

    import matplotlib.pyplot as plt
    plt.plot(np.arange(len(values)), values/max(values), c='r')
    plt.plot(np.arange(len(values)), score, c='b', alpha=0.5)
    plt.show()


if __name__ == '__main__':
    main()
