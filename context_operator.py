# -*- coding: utf-8 -*-


class ContextOperator(object):
    def __init__(self, max_left_semi_contexts_length):
        self.max_left_semi_contexts_length = max_left_semi_contexts_length

        self.facts_dicts = [{}, {}]
        self.semi_context_dicts = [{}, {}]
        self.semi_context_values_lists = [[], []]
        self.crossed_semi_contexts_lists = [[], []]
        self.contexts_values_list = []

        self.new_context_id = False

    def get_context_by_facts(self, new_contexts_list, zerolevel=0):
        """
        The function which determines by the complete facts list whether the context
        is already saved to the memory. If the context is not found the function
        immediately creates such. To optimize speed and volume of the occupied memory
        the contexts are divided into semi-contexts as several contexts can contain
        the same facts set in its left and right parts.

        @param new_contexts_list:       list of potentially new contexts

        @param zerolevel:               flag indicating the context type in
                                        transmitted list

        @return :   depending on the type of  potentially new context transmitted as
                    an input parameters the function returns either:
                    а) flag indicating that the transmitted zero-level context is
                    a new/existing one;
                    or:
                    b) number of the really new contexts that have been saved to the
                    context memory.
        """

        num_added_contexts = 0

        for left_facts, right_facts in new_contexts_list:
            left_hash = left_facts.__hash__()
            right_hash = right_facts.__hash__()

            next_left_semi_context_number = len(self.semi_context_dicts[0])
            left_semi_context_id = self.semi_context_dicts[0].setdefault(left_hash, next_left_semi_context_number)
            if left_semi_context_id == next_left_semi_context_number:
                left_semi_context_values = [[], len(left_facts), 0, {}]
                self.semi_context_values_lists[0].append(left_semi_context_values)
                for fact in left_facts:
                    semi_context_list = self.facts_dicts[0].setdefault(fact, [])
                    semi_context_list.append(left_semi_context_values)

            next_right_semi_context_number = len(self.semi_context_dicts[1])
            right_semi_context_id = self.semi_context_dicts[1].setdefault(right_hash, next_right_semi_context_number)
            if right_semi_context_id == next_right_semi_context_number:
                right_semi_context_values = [[], len(right_facts), 0]
                self.semi_context_values_lists[1].append(right_semi_context_values)
                for fact in right_facts:
                    semi_context_list = self.facts_dicts[1].setdefault(fact, [])
                    semi_context_list.append(right_semi_context_values)

            next_free_context_id_number = len(self.contexts_values_list)
            context_id = self.semi_context_values_lists[0][left_semi_context_id][3].setdefault(right_semi_context_id, next_free_context_id_number)

            if context_id == next_free_context_id_number:
                num_added_contexts += 1
                context_values = [0, 0, 0, right_facts, zerolevel, left_hash, right_hash]

                self.contexts_values_list.append(context_values)
                if zerolevel:
                    self.new_context_id = context_id
                    return True
            else:
                context_values = self.contexts_values_list[context_id]

                if zerolevel:
                    context_values[4] = 1
                    return False

        return num_added_contexts

    def cross_contexts(self, left_or_right, facts_list, new_context_flag=False, potential_new_contexts=[]):
        if left_or_right == 0:
            if len(potential_new_contexts) > 0:
                num_new_contexts = self.get_context_by_facts(potential_new_contexts)
            else:
                num_new_contexts = 0
            max_pred_weight = 0.0
            new_predictions = set()
            prediction_contexts = []

        for semi_context_values in self.crossed_semi_contexts_lists[left_or_right]:
            semi_context_values[0] = []
            semi_context_values[2] = 0

        for fact in facts_list:
            for semi_context_values in self.facts_dicts[left_or_right].get(fact, []):
                semi_context_values[0].append(fact)

        new_crossed_values = []

        for semi_context_values in self.semi_context_values_lists[left_or_right]:
            len_semi_context_values0 = len(semi_context_values[0])
            semi_context_values[2] = len_semi_context_values0
            if len_semi_context_values0 > 0:
                new_crossed_values.append(semi_context_values)
                if left_or_right == 0 and semi_context_values[1] == len_semi_context_values0:
                    for context_id in semi_context_values[3].itervalues():
                        context_values = self.contexts_values_list[context_id]

                        curr_pred_weight = context_values[1] / float(context_values[0]) if context_values[0] > 0 else 0.0

                        if curr_pred_weight > max_pred_weight:
                            max_pred_weight = curr_pred_weight
                            prediction_contexts = [context_values]

                        elif curr_pred_weight == max_pred_weight:
                            prediction_contexts.append(context_values)

        self.crossed_semi_contexts_lists[left_or_right] = new_crossed_values

        if left_or_right:
            return self.update_contexts_and_get_active(new_context_flag)
        else:
            [new_predictions.update(context_values[3]) for context_values in prediction_contexts]

            return num_new_contexts, new_predictions

    def update_contexts_and_get_active(self, new_context_flag):
        """
        This function reviews the list of previously selected left semi-contexts,
        updates the prediction results value of all contexts, including left
        semi-contexts, creates the list of potentially new contexts resulted from
        intersection between zero-level contexts, determines the contexts that
        coincide with the input data and require activation, prepares the values
        for calculating anomaly value.

        @param new_context_flag:        flag indicating that a new zero-level
                                        context is not recorded at the current
                                        step, which means that all contexts
                                        already exist and there is no need to
                                        create new ones.

        @return active_contexts:        list of identifiers of the contexts which
                                        completely coincide with the input stream,
                                        should be considered active and be
                                        recorded to the input stream of “neurons”

        @return potential_new_context_list:  list of contexts based on intersection
                                        between the left and the right zero-level
                                        semi-contexts, which are potentially new
                                        contexts requiring saving to the context
                                        memory
        """

        active_contexts = []
        num_selected_context = 0

        potential_new_context_list = []

        for left_semi_context_values in self.crossed_semi_contexts_lists[0]:
            for right_semi_context_id, context_id in left_semi_context_values[3].iteritems():

                if self.new_context_id != context_id:
                    context_values = self.contexts_values_list[context_id]
                    right_semi_context_value0,  right_semi_context_value1, right_semi_context_value2 = self.semi_context_values_lists[1][right_semi_context_id]

                    if left_semi_context_values[1] == left_semi_context_values[2]:
                        num_selected_context += 1
                        context_values[0] += right_semi_context_value1

                        if right_semi_context_value2 > 0:
                            context_values[1] += right_semi_context_value2

                            if right_semi_context_value1 == right_semi_context_value2:
                                context_values[2] += 1
                                active_contexts.append([context_id, context_values[2], context_values[5], context_values[6]])

                            elif context_values[4] and new_context_flag and left_semi_context_values[2] <= self.max_left_semi_contexts_length:
                                potential_new_context_list.append(tuple([tuple(left_semi_context_values[0]), tuple(right_semi_context_value0)]))

                    elif context_values[4] and new_context_flag and right_semi_context_value2 > 0 and left_semi_context_values[2] <= self.max_left_semi_contexts_length:
                        potential_new_context_list.append(tuple([tuple(left_semi_context_values[0]), tuple(right_semi_context_value0)]))

        self.new_context_id = False

        return active_contexts, num_selected_context, potential_new_context_list
