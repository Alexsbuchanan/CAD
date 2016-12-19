# -*- coding: utf-8 -*-

import csv
import datetime
import glob
import math
import multiprocessing
import os
import subprocess

from cad_ose import ContextualAnomalyDetector


def main():
    git_version = subprocess.check_output(['git', 'describe', '--always']).strip()

    params = {
        'base_data_dir':                 '../NAB/data',
        'base_results_dir':              '../NAB/results',
        'null_results_dir':              '../NAB/results/null',
        'proj_dir_descr':                'CAD-{0}'.format(git_version),
        'max_left_semi_contexts_length':  7,
        'max_active_neurons_num':        15,
        'num_norm_value_bits':            3,
        'base_threshold':                 0.75,
    }

    full_file_names = glob.glob(os.path.join(params['base_data_dir'], '**/*.csv'))

    def process_wrap(args):
        process(*args, **params)

    pool = multiprocessing.Pool()
    pool.map_async(process_wrap, enumerate(full_file_names)).get(999999999)


def process(file_number,
            full_file_name,
            base_data_dir,
            base_results_dir,
            null_results_dir,
            proj_dir_descr,
            max_left_semi_contexts_length,
            max_active_neurons_num,
            num_norm_value_bits,
            base_threshold,
            ):
        print("-----------------------------------------")
        print("[ " + str(file_number+1) + " ] " + full_file_name)

        min_value = float("inf")
        max_value = -float("inf")
        with open(full_file_name, 'rb') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            next(csv_reader)
            for row_number, row in enumerate(csv_reader, start=1):
                input_data_value = float(row[1])
                min_value = min(input_data_value, min_value)
                max_value = max(input_data_value, max_value)

        learning_period = min(math.floor(0.15 * row_number), 0.15 * 5000)

        print("min_value = " + str(min_value) + " : max_value = " + str(max_value))

        cad = ContextualAnomalyDetector(
            min_value=min_value,
            max_value=max_value,
            base_threshold=base_threshold,
            rest_period=learning_period / 5.0,
            max_left_semi_contexts_length=max_left_semi_contexts_length,
            max_active_neurons_num=max_active_neurons_num,
            num_norm_value_bits=num_norm_value_bits
        )

        anomaly_array = []
        num_steps = 0

        out_file_dsc = full_file_name[len(base_data_dir) + 1:].split("/")

        labels_file = open(null_results_dir + "/" + out_file_dsc[0] + "/" + "null_" + out_file_dsc[1], 'rb')
        csv_labels_reader = csv.reader(labels_file, delimiter=',')
        next(csv_labels_reader)

        with open(full_file_name, 'rb') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            next(csv_reader)
            for row in csv_reader:
                num_steps += 1
                current_label = next(csv_labels_reader)[3]

                input_data_date = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                input_data_value = float(row[1])
                input_data = {"timestamp": input_data_date, "value": input_data_value}

                results = cad.get_anomaly_score(input_data)
                anomaly_array.append([num_steps, row[0], row[1], current_label, [results]])

        new_file_name = base_results_dir + "/" + proj_dir_descr + "/" + out_file_dsc[0] + "/" + proj_dir_descr + "_" + out_file_dsc[1]
        ensure_dir(new_file_name)
        with open(new_file_name, 'w') as csv_out_file:
            csv_out_file.write("timestamp,value,anomaly_score,label\n")
            for anomaly_scores in anomaly_array:
                csv_out_file.write(
                    anomaly_scores[1] + "," + anomaly_scores[2] + "," + str(anomaly_scores[4][0]) + "," +
                    anomaly_scores[3] + "\n")
        print ("saved to: " + new_file_name)


def ensure_dir(path):
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)


if __name__ == '__main__':
    main()
