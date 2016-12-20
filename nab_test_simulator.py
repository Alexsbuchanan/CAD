# -*- coding: utf-8 -*-

import csv
import datetime
import glob
import math
import multiprocessing
import os
import subprocess
import time

from cad_ose import ContextualAnomalyDetector


def main():
    git_version = subprocess.check_output(['git', 'describe', '--always']).strip()
    detector_name = 'CAD-{}'.format(git_version)

    params = {
        'base_data_dir':              '../NAB/data',
        'base_results_dir':           '../NAB/results',
        'null_results_dir':           '../NAB/results/null',
        'proj_dir_descr':             detector_name,
        'max_left_semi_ctxs_length':  7,
        'max_active_neurons_num':     15,
        'num_norm_value_bits':        3,
        'base_threshold':             0.75,
    }

    full_file_names = glob.glob(os.path.join(params['base_data_dir'], '**/*.csv'))

    pool = multiprocessing.Pool()
    pool.map_async(process_wrap,  ((i, fn, params) for i, fn in enumerate(full_file_names))).get(999999999)

    os.chdir('../NAB')
    subprocess.check_call(['python', 'run.py', '-d', detector_name, '--score', '--normalize', '--skipConfirmation'])


def process_wrap(args):
    i, fn, params = args
    process(i, fn, **params)


def process(file_number,
            full_file_name,
            base_data_dir,
            base_results_dir,
            null_results_dir,
            proj_dir_descr,
            max_left_semi_ctxs_length,
            max_active_neurons_num,
            num_norm_value_bits,
            base_threshold,
            ):
        nrows, min_, max_ = data_stats(full_file_name)

        print '  [{}]\t{:.3f}\t{:.3f}\t{}'.format(file_number+1, min_, max_, os.path.basename(full_file_name))

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

        out_file_dsc = full_file_name[len(base_data_dir) + 1:].split("/")

        labels_file = open(os.path.join(null_results_dir, out_file_dsc[0], "null_" + out_file_dsc[1]), 'rb')
        csv_labels_reader = csv.reader(labels_file)
        next(csv_labels_reader)

        start = time.time()

        anomaly_data = []
        with open(full_file_name, 'rb') as f:
            r = csv.reader(f)
            next(r)
            for row in r:
                input_data = {
                    'timestamp': datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S'),
                    'value':     float(row[1]),
                }

                score = cad.get_anomaly_score(input_data)

                cur_label = next(csv_labels_reader)[3]
                anomaly_data.append([row[0], row[1], score, cur_label])

        out_file_name = os.path.join(base_results_dir, proj_dir_descr, out_file_dsc[0], proj_dir_descr + "_" + out_file_dsc[1])
        write_anomaly_data(out_file_name, anomaly_data)

        dt = datetime.timedelta(seconds=time.time()-start)
        print '✓ [{}]\t{}\t{}\t{}'.format(file_number + 1, os.path.basename(full_file_name), dt, dt / len(anomaly_data))


def data_stats(filename):
    min_ = float('inf')
    max_ = -float('inf')

    with open(filename, 'rb') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)
        for i, row in enumerate(csv_reader):
            v = float(row[1])
            min_ = min(v, min_)
            max_ = max(v, max_)

    return i + 1, min_, max_


def write_anomaly_data(filename, anomaly_data):
    ensure_dir(filename)
    with open(filename, 'w') as f:
        w = csv.writer(f)
        w.writerow(['timestamp', 'value', 'anomaly_score', 'label'])
        w.writerows(anomaly_data)


def ensure_dir(path):
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)


if __name__ == '__main__':
    main()
