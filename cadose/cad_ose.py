# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Contextual Anomaly Detector — Open Source Edition
#
# Copyright © 2016 Mikhail Smirnov <smirmik@gmail.com>
# Copyright © 2016 Gregory Petrosyan <gregory.petrosyan@gmail.com>
# Copyright © 2019 Alexander Buchanan <alexsbuchanan@gmail.com>
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

from timeit import default_timer as timer
from cadose.context_operator import ContextOperator
import numpy as np

class ContextualAnomalyDetector(object):
    def __init__(self, min_value, max_value, base_threshold, rest_period,
                 max_lsemi_ctxs_len, max_active_neurons_num,
                 num_norm_value_bits):
        """
        This class is used to first train the detector based on facts and
        contexts of previous known data.
        min_value and max_value are used to compute the min_value_step param
        which is then used to scale the input appropriately. Note that I we
        have a general sense of what the minimum and maximum values our data
        might have it may very well be worth it to assign these parameters
        correctly.

        For example if we are looking at CPU usage as a percentage, the optimal
        values for min and max value are 0 and 100 respectively.

        The base threshold here is used as a threshold in conjunction with
        the rest period to determine if we should alert about an anomaly again.
        I.e it really is just a way to aid in preventing alert fatigue or
        sending alerts about events that are already know repeatedly.

        The maximum left semi contexts is TODO: find out what max_lsemi_ctx_len
                                                is
        Max active neurons is a cap on the neurons that max be activated at
        any given time. Num norm value bits controls

        :param min_value: Minimum value in the data set TODO Verify this
        :param max_value: Maximum value in the data set TODO Verify this
        :param base_threshold: The base threshold to declare an anomaly
        :param rest_period: The rest period...? TODO Figure this out
        :param max_lsemi_ctxs_len: Max left semi contexts length TODO ^
        :param max_active_neurons_num: Maximum number of active neurons
        :param num_norm_value_bits: Number of norm values bits (TODO what?)
        """
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

        # ctx_operator is TODO is what?
        self.ctx_operator = ContextOperator(max_lsemi_ctxs_len)

        self.potential_new_ctxs = []

        self.last_predicted_facts = []

        self.result_values_history = [1.0]

        # DEBUG
        self.flags = []

        self.avg_time = []

    def step(self, facts):  # facts must be distinct and sorted
        """
        This function updates the contexts of the left and right side based
        on the dictionary 'facts' that are passed. It is called internally by
        the get anomaly score function, but may be called if you do not want
        an anomaly score and only wish to train the NN.

        It first creates potential new facts to append to the left side at the
        zeroth level. (TODO: What is zero level?)
        Then it runs a cross context operator on the RHS contexts. This resets
        the contexts on the right hand side and returns the activated contexts,
        the number of selected contexts, the potential new contexts and the
        number of contexts.

        The number of unique potential contexts are calculated.

        The percent of selected contexts that are active is also calculated. If
        there were so selected contexts it defaults to 0.

        The active contexts are then sorted by how often they were activated,
        (lowest -> highest). The maximum amount of neurons (user defined) are
        selected and their context ID's are converted to facts via the
        operation:
        .. math:
            F \equals \{2^{31} + fact \rvert fact \in N\}
        Where here N is the set of active neuron facts, and F is the new set
        of facts.
        The left side is then updated with these new facts, and sorted from
        lowest to highest. We then do a cross context of facts on the left,
        yielding the number of new contexts and the predictions it yielded.

        :param facts:
        :return:
        """
        # Say the potential new zero level contexts are the left facts
        # and the right facts if there are left facts
        pot_new_zero_level_ctx = [(self.left_facts_group,
                                   facts)] \
            if self.left_facts_group and facts else []

        # Get the active contexts, the number of selected contexts and
        # potential new contexts, and their flag from context operator
        # TODO: Figure out what ctx_operator.cross_ctxs_right is
        active_ctxs, num_selected_ctx, potential_new_ctxs, new_ctx_flag = \
            self.ctx_operator.cross_ctxs_right(
                    facts=facts,
                    pot_new_zero_level_ctx=pot_new_zero_level_ctx
            )

        # Get the number of unique potential new contexts
        num_uniq_pot_new_ctx = len(set(potential_new_ctxs)
                                   .union(pot_new_zero_level_ctx))

        # Get the percentage of currently active contexts and check if
        # the number is 0
        if num_selected_ctx:
            pct_selected_ctx_active = len(active_ctxs) / float(
                    num_selected_ctx)
        else:
            pct_selected_ctx_active = 0.0

        # Get the active contexts (Sorted)
        active_ctxs = sorted(active_ctxs,
                             key=lambda ctx: ctx.ctx_num_activations)

        # Get the active Neurons ID from the contexts in active Neurons
        # This returns the most active Neurons and only the number of the
        # specified by self.max_active_neurons_num
        active_neurons = [actx.ctx_id for actx in
                          active_ctxs[-self.max_active_neurons_num:]]

        # Create the 'facts' for the new Neuron
        curr_neur_facts = set(2 ** 31 + fact for fact in active_neurons)

        # Replace the left facts with the new current facts we just found
        self.left_facts_group = set()
        self.left_facts_group.update(facts, curr_neur_facts)
        self.left_facts_group = tuple(sorted(self.left_facts_group))

        # With our new left side facts 'cross contexts left'
        # TODO: Figure out wtf that means
        #       See ctx_operator.cross_ctxs_left
        num_new_ctxs, new_predictions = self.ctx_operator.cross_ctxs_left(
                facts=self.left_facts_group,
                potential_new_ctxs=potential_new_ctxs
        )

        # If the cross_ctxs_right returns new_ctx_flag >= 1, add one to
        # num_new ctxs
        num_new_ctxs += 1 if new_ctx_flag else 0

        # Get the percentage added to the unique potential new contexts
        if new_ctx_flag and num_uniq_pot_new_ctx > 0:
            pct_pot_uniq_ctx_new = num_new_ctxs / float(
                    num_uniq_pot_new_ctx)
        else:
            pct_pot_uniq_ctx_new = 0.0

        # DEBUG
        self.flags.append(new_ctx_flag)

        return new_predictions, (
                pct_selected_ctx_active, pct_pot_uniq_ctx_new)

    def get_anomaly_score(self, input_data):
        """
        This is the main function of the ContextualAnomalyDetector class,
        it takes in purely a value (it does not care about a date) and outputs
        and anomaly 'score'. It first converts the input to a scaled integer
        based on the minimum step value, then converts that integer to a binary
        value which is created into what are called facts. A prediction error
        is then calculated and scaled by the maximum binary value. Then the
        facts are run through the step() function. This returns anomaly values,
        which are then used to create an anomaly score based on the previous 2
        anomaly values. That score is then returned

        :param input_data: A numeric value representative of the data
        :return: float, and anomaly score.
        """
        start = timer()

        # Min-max scale the normal input value and scale it by the maximum
        # binary value
        norm_input_value = int((input_data - self.min_value)
                               / self.min_value_step)

        # TODO: Add support for negative values

        # Conver the normal input value to a bianry string representation
        # strip the '0b' and add zeros on the left up to the number of normal
        # value bits
        bin_input_norm_value = format(norm_input_value, 'b').rjust(
                self.num_norm_value_bits, '0')

        # Create the 'facts' which is a tuple derived from the sorted input
        # reversed bits.
        # TODO: Write this out in LaTeX to understand what the hell this
        #       encoding truly is
        # Also note the original implementation had some 'magic' where the line
        # would be ... s_num * 2 ** 16 ... and in prediction error below
        # it would be ... 2 ** ((fact - 2**16)/2.0).
        # facts = tuple(
        #         s_num * 2 + (1 if cur_sym == '1' else 0) for s_num, cursym in
        #         enumerate(reversed(bin_input_norm_value)))
        facts = tuple(
                set(65536 + s_num * 2 * + int(cur_sym) for s_num, cur_sym in
                    enumerate(reversed(bin_input_norm_value))))

        # Sum the prediction error for all facts that weren't just predicted
        # Note it is then scaled by the maximum binary value
        prediction_error = sum(2 ** ((fact - 65536) / 2.0) for fact in facts if
                               fact not in self.last_predicted_facts) \
            / self.max_bin_value

        # Step forward in the facts
        self.last_predicted_facts, anomaly_values = self.step(facts)

        # Calculate the anomaly score for this individual value
        # if prediction_error > 0:
        current_anomaly_score = (1.0 - anomaly_values[0]
                                 + anomaly_values[1]) / 2.0
        # else:
        #     current_anomaly_score = 0.0

        if max(self.result_values_history[
               -int(self.rest_period):]) < self.base_threshold:
            returned_anomaly_score = current_anomaly_score
        else:
            returned_anomaly_score = 0.0

        self.result_values_history.append(current_anomaly_score)

        # if returned_anomaly_score < self.base_threshold / 2.0:
        #     returned_anomaly_score = 0.0

        self.avg_time.append(timer() - start)
        # return current_anomaly_score
        return returned_anomaly_score

    def get_avg_time(self):
        return np.mean(self.avg_time)
