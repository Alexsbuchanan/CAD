# -*- coding: utf-8 -*-

import csv
import datetime
import math
import os
import subprocess

from cad_ose import ContextualAnomalyDetector


if __name__ == '__main__':
    test_set = 1

    base_data_dir = "../NAB/data"

    base_results_dir = "../NAB/results"
    null_results_dir = base_results_dir + "/null"

    num_result_types = 1
    start_anomaly_value_number = 0

    if test_set == 1:
        max_left_semi_contexts_length = 7
        max_active_neurons_num = 15
        num_norm_value_bits = 3
        base_threshold = 0.75

    elif test_set == 0:
        max_left_semi_contexts_length = 8
        max_active_neurons_num = 16
        num_norm_value_bits = 3
        base_threshold = 1.0

    project_dir_descriptors = []
    git_version = subprocess.check_output(['git', 'describe', '--always']).strip()
    for values_version in xrange(num_result_types):
        project_dir_descriptors.append("CAD-{0}-Set{1:1d}".format(git_version, test_set))

    data_dir_tree = os.walk(base_data_dir)

    dir_names = []
    full_file_names = []

    for i, dir_descr in enumerate(data_dir_tree):
        if i == 0:
            dir_names = dir_descr[1]
        else:
            for file_name in dir_descr[2]:
                full_file_names.append(dir_descr[0] + "/" + file_name)

    for proj_dir_descr in project_dir_descriptors:
        for directory in dir_names:
            os.makedirs(base_results_dir + "/" + proj_dir_descr + "/" + directory)

    for file_number, full_file_name in enumerate(full_file_names, start=1):
        print("-----------------------------------------")
        print("[ " + str(file_number) + " ] " + full_file_name)

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

        for i, proj_dir_descr in enumerate(project_dir_descriptors, start=start_anomaly_value_number):
            new_file_name = base_results_dir + "/" + proj_dir_descr + "/" + out_file_dsc[
                0] + "/" + proj_dir_descr + "_" + out_file_dsc[1]
            with open(new_file_name, 'w') as csv_out_file:
                csv_out_file.write("timestamp,value,anomaly_score,label\n")
                for anomaly_scores in anomaly_array:
                    csv_out_file.write(
                        anomaly_scores[1] + "," + anomaly_scores[2] + "," + str(anomaly_scores[4][i]) + "," +
                        anomaly_scores[3] + "\n")
            print ("saved to: " + new_file_name)
