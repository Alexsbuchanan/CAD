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

from context_operator import ContextOperator


class ContextualAnomalyDetector(object):
    def __init__(self, min_value, max_value, base_threshold, rest_period, max_lsemi_ctxs_len, max_active_neurons_num, num_norm_value_bits):
        self.min_value = float(min_value)
        self.max_value = float(max_value)
        self.rest_period = rest_period
        self.base_threshold = base_threshold
        self.max_active_neurons_num = max_active_neurons_num
        self.num_norm_value_bits = num_norm_value_bits

        self.max_bin_value = 2 ** self.num_norm_value_bits - 1.0
        self.full_value_range = self.max_value - self.min_value
        if self.full_value_range == 0.0:
            self.full_value_range = self.max_bin_value
        self.min_value_step = self.full_value_range / self.max_bin_value

        self.left_facts_group = tuple()

        self.ctx_operator = ContextOperator(max_lsemi_ctxs_len)

        self.potential_new_ctxs = []

        self.last_predicted_facts = []
        self.result_values_history = [1.0]

    def step(self, facts):  # facts must be distinct and sorted
        pot_new_zero_level_ctx = [(self.left_facts_group, facts)] if self.left_facts_group and facts else []

        active_ctxs, num_selected_ctx, potential_new_ctxs, new_ctx_flag = self.ctx_operator.cross_ctxs_right(
                                                                        facts=facts,
                                                                        pot_new_zero_level_ctx=pot_new_zero_level_ctx
                                                                    )

        num_uniq_pot_new_ctx = len(set(potential_new_ctxs).union(pot_new_zero_level_ctx))

        percent_selected_ctx_active = len(active_ctxs) / float(num_selected_ctx) if num_selected_ctx > 0 else 0.0

        active_ctxs = sorted(active_ctxs, key=lambda ctx: ctx.ctx_num_activations)
        active_neurons = [actx.ctx_id for actx in active_ctxs[-self.max_active_neurons_num:]]

        curr_neur_facts = set(2**31 + fact for fact in active_neurons)

        self.left_facts_group = set()
        self.left_facts_group.update(facts, curr_neur_facts)
        self.left_facts_group = tuple(sorted(self.left_facts_group))

        num_new_ctxs, new_predictions = self.ctx_operator.cross_ctxs_left(
                                                        facts=self.left_facts_group,
                                                        potential_new_ctxs=potential_new_ctxs
                                                    )

        num_new_ctxs += 1 if new_ctx_flag else 0

        percent_added_ctx_to_uniq_pot_new = num_new_ctxs / float(num_uniq_pot_new_ctx) if new_ctx_flag and num_uniq_pot_new_ctx > 0 else 0.0

        return new_predictions, (percent_selected_ctx_active, percent_added_ctx_to_uniq_pot_new)

    def get_anomaly_score(self, input_data):
        norm_input_value = int((input_data['value'] - self.min_value) / self.min_value_step)
        bin_input_norm_value = bin(norm_input_value).lstrip('0b').rjust(self.num_norm_value_bits, '0')

        facts = tuple(s_num * 2 + (1 if cur_sym == '1' else 0) for s_num, cur_sym in enumerate(reversed(bin_input_norm_value)))
        prediction_error = sum(2 ** (fact / 2.0) for fact in facts if fact not in self.last_predicted_facts) / self.max_bin_value

        self.last_predicted_facts, anomaly_values = self.step(facts)

        current_anomaly_score = (1.0 - anomaly_values[0] + anomaly_values[1]) / 2.0 if prediction_error > 0 else 0.0

        returned_anomaly_score = current_anomaly_score if max(self.result_values_history[-int(self.rest_period):]) < self.base_threshold else 0.0
        self.result_values_history.append(current_anomaly_score)

        return returned_anomaly_score
