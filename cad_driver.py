#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import math
import sys

from cad_ose import ContextualAnomalyDetector


def main():
    nrows = int(sys.argv[1])
    min_ = float(sys.argv[2])
    max_ = float(sys.argv[3])

    base_threshold = 0.75
    max_left_semi_ctxs_length = 7
    max_active_neurons_num = 15
    num_norm_value_bits = 3

    run(nrows=nrows,
        min_=min_,
        max_=max_,
        base_threshold=base_threshold,
        max_left_semi_ctxs_length=max_left_semi_ctxs_length,
        max_active_neurons_num=max_active_neurons_num,
        num_norm_value_bits=num_norm_value_bits)


def run(nrows, min_, max_, base_threshold, max_left_semi_ctxs_length, max_active_neurons_num, num_norm_value_bits):
    learning_period = min(math.floor(0.15 * nrows), 0.15 * 5000)

    cad = ContextualAnomalyDetector(
        min_value=min_,
        max_value=max_,
        base_threshold=base_threshold,
        rest_period=learning_period / 5.0,
        max_left_semi_ctxs_length=max_left_semi_ctxs_length,
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
