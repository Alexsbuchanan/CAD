# -*- coding: utf-8 -*-

from context_operator import ContextOperator


class ContextualAnomalyDetector(object):
    def __init__(self, min_value, max_value, base_threshold=0.75, rest_period=30, max_left_semi_contexts_length=7, max_active_neurons_num=15, num_norm_value_bits=3):
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

        self.context_operator = ContextOperator(max_left_semi_contexts_length)

        self.potential_new_contexts = []

        self.last_predicted_facts = []
        self.result_values_history = [1.0]

    def step(self, input_facts):
        curr_sens_facts = tuple(sorted(set(input_facts)))

        if len(self.left_facts_group) > 0 and len(curr_sens_facts) > 0:
            pot_new_zero_level_context = tuple([self.left_facts_group, curr_sens_facts])
            new_context_flag = self.context_operator.get_context_by_facts([pot_new_zero_level_context], zerolevel=1)
        else:
            pot_new_zero_level_context = False
            new_context_flag = False

        active_contexts, num_selected_context, potential_new_context_list = self.context_operator.cross_contexts(
                                                                            left_or_right=1,
                                                                            facts_list=curr_sens_facts,
                                                                            new_context_flag=new_context_flag
                                                                        )

        num_uniq_pot_new_context = len(set(potential_new_context_list).union([pot_new_zero_level_context]) if pot_new_zero_level_context else set(potential_new_context_list))

        percent_selected_context_active = len(active_contexts) / float(num_selected_context) if num_selected_context > 0 else 0.0

        active_contexts = sorted(active_contexts, cmp=compare_contexts)
        active_neurons = [activeContextInfo[0] for activeContextInfo in active_contexts[-self.max_active_neurons_num:]]

        curr_neur_facts = set([2 ** 31 + fact for fact in active_neurons])

        self.left_facts_group = set()
        self.left_facts_group.update(curr_sens_facts, curr_neur_facts)
        self.left_facts_group = tuple(sorted(self.left_facts_group))

        num_new_contexts, new_predictions = self.context_operator.cross_contexts(
                                                        left_or_right=0,
                                                        facts_list=self.left_facts_group,
                                                        potential_new_contexts=potential_new_context_list
                                                    )

        num_new_contexts += 1 if new_context_flag else 0

        percent_added_context_to_uniq_pot_new = num_new_contexts / float(num_uniq_pot_new_context) if new_context_flag and num_uniq_pot_new_context > 0 else 0.0

        return new_predictions, [percent_selected_context_active, percent_added_context_to_uniq_pot_new]

    def get_anomaly_score(self, input_data):
        norm_input_value = int((input_data["value"] - self.min_value) / self.min_value_step)
        bin_input_norm_value = bin(norm_input_value).lstrip("0b").rjust(self.num_norm_value_bits, "0")

        out_sens = set([2**16 + s_num * 2 + (1 if cur_sym == "1" else 0) for s_num, cur_sym in enumerate(reversed(bin_input_norm_value))])

        prediction_error = sum([2 ** ((fact-65536) / 2.0) for fact in out_sens if fact not in self.last_predicted_facts]) / self.max_bin_value

        self.last_predicted_facts, anomalyValues = self.step(out_sens)

        current_anomaly_score = (1.0 - anomalyValues[0] + anomalyValues[1]) / 2.0 if prediction_error > 0 else 0.0

        returned_anomaly_score = current_anomaly_score if max(self.result_values_history[-int(self.rest_period):]) < self.base_threshold else 0.0
        self.result_values_history.append(current_anomaly_score)

        return returned_anomaly_score


def compare_contexts(x, y):
    if cmp(x[1], y[1]) != 0:
        return cmp(x[1], y[1])
    if cmp(x[2], y[2]) != 0:
        return cmp(x[2], y[2])
    return cmp(x[3], y[3])
