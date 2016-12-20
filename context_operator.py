# -*- coding: utf-8 -*-

import recordclass


Half = recordclass.recordclass('Half', 'facts_dict semi_ctx_dict semi_ctx_values_list crossed_semi_ctxs_list')


class ContextOperator(object):
    def __init__(self, max_left_semi_ctxs_length):
        self.max_left_semi_ctxs_length = max_left_semi_ctxs_length

        self.left = Half({}, {}, [], [])
        self.right = Half({}, {}, [], [])
        self.ctxs_values_list = []

        self.new_ctx_id = False

    def choose_half(self, left_or_right):
        return self.left if left_or_right == 0 else self.right

    def get_ctx_by_facts(self, new_ctxs_list, zerolevel):
        """
        The function which determines by the complete facts list whether the context
        is already saved to the memory. If the context is not found the function
        immediately creates such. To optimize speed and volume of the occupied memory
        the contexts are divided into semi-contexts as several contexts can contain
        the same facts set in its left and right parts.

        @param new_ctxs_list:       list of potentially new contexts

        @param zerolevel:           flag indicating the context type in transmitted list

        @return :   depending on the type of  potentially new context transmitted as
                    an input parameters the function returns either:
                    а) flag indicating that the transmitted zero-level context is a new/existing one;
                    or:
                    b) number of the really new contexts that have been saved to the context memory.
        """

        num_added_ctxs = 0

        for left_facts, right_facts in new_ctxs_list:
            left_hash = hash(left_facts)
            right_hash = hash(right_facts)

            next_left_semi_ctx_number = len(self.left.semi_ctx_dict)
            left_semi_ctx_id = self.left.semi_ctx_dict.setdefault(left_hash, next_left_semi_ctx_number)
            if left_semi_ctx_id == next_left_semi_ctx_number:
                left_semi_ctx_values = [[], len(left_facts), 0, {}]
                self.left.semi_ctx_values_list.append(left_semi_ctx_values)
                for fact in left_facts:
                    semi_ctx_list = self.left.facts_dict.setdefault(fact, [])
                    semi_ctx_list.append(left_semi_ctx_values)

            next_right_semi_ctx_number = len(self.right.semi_ctx_dict)
            right_semi_ctx_id = self.right.semi_ctx_dict.setdefault(right_hash, next_right_semi_ctx_number)
            if right_semi_ctx_id == next_right_semi_ctx_number:
                right_semi_ctx_values = [[], len(right_facts), 0]
                self.right.semi_ctx_values_list.append(right_semi_ctx_values)
                for fact in right_facts:
                    semi_ctx_list = self.right.facts_dict.setdefault(fact, [])
                    semi_ctx_list.append(right_semi_ctx_values)

            next_free_ctx_id_number = len(self.ctxs_values_list)
            ctx_id = self.left.semi_ctx_values_list[left_semi_ctx_id][3].setdefault(right_semi_ctx_id, next_free_ctx_id_number)

            if ctx_id == next_free_ctx_id_number:
                num_added_ctxs += 1
                ctx_values = [0, 0, 0, right_facts, zerolevel, left_hash, right_hash]

                self.ctxs_values_list.append(ctx_values)
                if zerolevel:
                    self.new_ctx_id = ctx_id
                    return True
            else:
                ctx_values = self.ctxs_values_list[ctx_id]

                if zerolevel:
                    ctx_values[4] = 1
                    return False

        return num_added_ctxs

    def cross_ctxs(self, left_or_right, facts_list, new_ctx_flag=False, potential_new_ctxs=None):
        if potential_new_ctxs is None:
            potential_new_ctxs = []

        if left_or_right == 0:
            if len(potential_new_ctxs) > 0:
                num_new_ctxs = self.get_ctx_by_facts(potential_new_ctxs, zerolevel=0)
            else:
                num_new_ctxs = 0
            max_pred_weight = 0.0
            new_predictions = set()
            prediction_ctxs = []

        semi = self.choose_half(left_or_right)

        for semi_ctx_values in semi.crossed_semi_ctxs_list:
            semi_ctx_values[0] = []
            semi_ctx_values[2] = 0

        for fact in facts_list:
            for semi_ctx_values in semi.facts_dict.get(fact, []):
                semi_ctx_values[0].append(fact)

        new_crossed_values = []

        for semi_ctx_values in semi.semi_ctx_values_list:
            len_semi_ctx_values0 = len(semi_ctx_values[0])
            semi_ctx_values[2] = len_semi_ctx_values0
            if len_semi_ctx_values0 > 0:
                new_crossed_values.append(semi_ctx_values)
                if left_or_right == 0 and semi_ctx_values[1] == len_semi_ctx_values0:
                    for ctx_id in semi_ctx_values[3].itervalues():
                        ctx_values = self.ctxs_values_list[ctx_id]

                        curr_pred_weight = ctx_values[1] / float(ctx_values[0]) if ctx_values[0] > 0 else 0.0

                        if curr_pred_weight > max_pred_weight:
                            max_pred_weight = curr_pred_weight
                            prediction_ctxs = [ctx_values]

                        elif curr_pred_weight == max_pred_weight:
                            prediction_ctxs.append(ctx_values)

        semi.crossed_semi_ctxs_list = new_crossed_values

        if left_or_right:
            return self.update_ctxs_and_get_active(new_ctx_flag)
        else:
            [new_predictions.update(ctx_values[3]) for ctx_values in prediction_ctxs]

            return num_new_ctxs, new_predictions

    def update_ctxs_and_get_active(self, new_ctx_flag):
        """
        This function reviews the list of previously selected left semi-contexts,
        updates the prediction results value of all contexts, including left
        semi-contexts, creates the list of potentially new contexts resulted from
        intersection between zero-level contexts, determines the contexts that
        coincide with the input data and require activation, prepares the values
        for calculating anomaly value.

        @param new_ctx_flag:            flag indicating that a new zero-level
                                        context is not recorded at the current
                                        step, which means that all contexts
                                        already exist and there is no need to
                                        create new ones.

        @return active_ctxs:            list of identifiers of the contexts which
                                        completely coincide with the input stream,
                                        should be considered active and be
                                        recorded to the input stream of “neurons”

        @return potential_new_ctx_list: list of contexts based on intersection
                                        between the left and the right zero-level
                                        semi-contexts, which are potentially new
                                        contexts requiring saving to the context
                                        memory
        """

        active_ctxs = []
        num_selected_ctx = 0

        potential_new_ctx_list = []

        for left_semi_ctx_values in self.left.crossed_semi_ctxs_list:
            for right_semi_ctx_id, ctx_id in left_semi_ctx_values[3].iteritems():

                if self.new_ctx_id != ctx_id:
                    ctx_values = self.ctxs_values_list[ctx_id]
                    right_semi_ctx_value0,  right_semi_ctx_value1, right_semi_ctx_value2 = self.right.semi_ctx_values_list[right_semi_ctx_id]

                    if left_semi_ctx_values[1] == left_semi_ctx_values[2]:
                        num_selected_ctx += 1
                        ctx_values[0] += right_semi_ctx_value1

                        if right_semi_ctx_value2 > 0:
                            ctx_values[1] += right_semi_ctx_value2

                            if right_semi_ctx_value1 == right_semi_ctx_value2:
                                ctx_values[2] += 1
                                active_ctxs.append([ctx_id, ctx_values[2], ctx_values[5], ctx_values[6]])

                            elif ctx_values[4] and new_ctx_flag and left_semi_ctx_values[2] <= self.max_left_semi_ctxs_length:
                                potential_new_ctx_list.append((tuple(left_semi_ctx_values[0]), tuple(right_semi_ctx_value0)))

                    elif ctx_values[4] and new_ctx_flag and right_semi_ctx_value2 > 0 and left_semi_ctx_values[2] <= self.max_left_semi_ctxs_length:
                        potential_new_ctx_list.append((tuple(left_semi_ctx_values[0]), tuple(right_semi_ctx_value0)))

        self.new_ctx_id = False

        return active_ctxs, num_selected_ctx, potential_new_ctx_list
