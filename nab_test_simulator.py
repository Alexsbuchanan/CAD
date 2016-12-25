#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Contextual Anomaly Detector — Open Source Edition
#
# Copyright © 2016 Mikhail Smirnov <smirmik@gmail.com>
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

import csv
import datetime
import errno
import glob
import multiprocessing
import os
import subprocess
import time


def main():
    git_version = subprocess.check_output(['git', 'describe', '--always']).strip()
    detector_name = 'CAD-{}'.format(git_version)

    params = {
        'base_data_dir':    '../NAB/data',
        'base_results_dir': '../NAB/results',
        'detector_cmd':     './cad_driver.py',
        'detector_name':    detector_name,
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
            detector_cmd,
            detector_name):
        nrows, min_, max_ = data_stats(full_file_name)

        print '  [{}]\t{:.3f}\t{:.3f}\t{}'.format(file_number + 1, min_, max_, os.path.basename(full_file_name))

        out_file_dsc = full_file_name[len(base_data_dir) + 1:].split("/")
        out_file_name = os.path.join(base_results_dir, detector_name, out_file_dsc[0], detector_name + "_" + out_file_dsc[1])
        ensure_dir(out_file_name)

        start = time.time()

        with open(full_file_name) as in_, open(out_file_name, 'w') as out:
            out.write('timestamp,value,anomaly_score\n')
            out.flush()
            subprocess.check_call([detector_cmd, str(nrows), str(min_), str(max_)], stdin=in_, stdout=out)

        dt = datetime.timedelta(seconds=time.time()-start)
        print '✓ [{}]\t{}\t{}'.format(file_number + 1, os.path.basename(full_file_name), dt)


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


def ensure_dir(path):
    dirname = os.path.dirname(path)
    try:
        os.makedirs(dirname)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(dirname):
            pass
        else:
            raise


if __name__ == '__main__':
    main()
