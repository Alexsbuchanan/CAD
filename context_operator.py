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

import recordclass


Half = recordclass.recordclass('Half', [
    'fact_to_semi_ctx',
    'facts_hash_to_semi_ctx_id',
    'semi_ctxs',  # index == semi_ctx_id
    'crossed_semi_ctxs'
])

Ctx = recordclass.recordclass('Ctx', [
    'c0',
    'c1',
    'c2',
    'right_facts',
    'zerolevel',
    'left_hash',
    'right_hash'
])

SemiCtx = recordclass.recordclass('SemiCtx', [
    'facts',
    'init_nfacts',
    'rsemi_ctx_id_to_ctx_id'
])

ActiveCtx = recordclass.recordclass('ActiveCtx', [
    'ctx_id',
    'ctx_c2'
])


class ContextOperator(object):
    def __init__(self, max_lsemi_ctxs_len):
        self.max_lsemi_ctxs_len = max_lsemi_ctxs_len

        self.left = Half({}, {}, [], [])
        self.right = Half({}, {}, [], [])
        self.ctxs = []

        self.new_ctx_id = False

    def _choose_half(self, left_or_right):
        return self.left if left_or_right == 0 else self.right

    def get_ctx_by_facts(self, new_ctxs, zerolevel):
        """
        The function which determines by the complete facts list whether the context
        is already saved to the memory. If the context is not found the function
        immediately creates such. To optimize speed and volume of the occupied memory
        the contexts are divided into semi-contexts as several contexts can contain
        the same facts set in its left and right parts.

        @param new_ctxs:       list of potentially new contexts

        @param zerolevel:      flag indicating the context type in transmitted list

        @return :   depending on the type of  potentially new context transmitted as
                    an input parameters the function returns either:
                    а) flag indicating that the transmitted zero-level context is a new/existing one;
                    or:
                    b) number of the really new contexts that have been saved to the context memory.
        """

        num_added_ctxs = 0

        for left_facts, right_facts in new_ctxs:
            left_hash = hash(left_facts)
            right_hash = hash(right_facts)

            def process_half(facts, hash_, half):
                next_semi_ctx_number = len(half.facts_hash_to_semi_ctx_id)
                semi_ctx_id = half.facts_hash_to_semi_ctx_id.setdefault(hash_, next_semi_ctx_number)
                if semi_ctx_id == next_semi_ctx_number:
                    semi_ctx = SemiCtx([], len(facts), {} if half == self.left else None)
                    half.semi_ctxs.append(semi_ctx)
                    for fact in facts:
                        semi_ctxs = half.fact_to_semi_ctx.setdefault(fact, [])
                        semi_ctxs.append(semi_ctx)
                return semi_ctx_id

            lsemi_ctx_id = process_half(left_facts, left_hash, self.left)
            rsemi_ctx_id = process_half(right_facts, right_hash, self.right)

            next_free_ctx_id_number = len(self.ctxs)
            ctx_id = self.left.semi_ctxs[lsemi_ctx_id].rsemi_ctx_id_to_ctx_id.setdefault(rsemi_ctx_id, next_free_ctx_id_number)

            if ctx_id == next_free_ctx_id_number:
                num_added_ctxs += 1
                ctx = Ctx(0, 0, 0, right_facts, zerolevel, left_hash, right_hash)

                self.ctxs.append(ctx)
                if zerolevel:
                    self.new_ctx_id = ctx_id
                    return True
            else:
                ctx = self.ctxs[ctx_id]

                if zerolevel:
                    ctx.zerolevel = 1
                    return False

        return num_added_ctxs

    def cross_ctxs(self, left_or_right, facts, new_ctx_flag=False, potential_new_ctxs=None):
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

        semi = self._choose_half(left_or_right)

        for semi_ctx in semi.crossed_semi_ctxs:
            semi_ctx.facts = []

        for fact in facts:
            for semi_ctx in semi.fact_to_semi_ctx.get(fact, []):
                semi_ctx.facts.append(fact)

        new_crossed_semi_ctxs = []

        for semi_ctx in semi.semi_ctxs:
            if len(semi_ctx.facts) > 0:
                new_crossed_semi_ctxs.append(semi_ctx)
                if left_or_right == 0 and semi_ctx.init_nfacts == len(semi_ctx.facts):
                    for ctx_id in semi_ctx.rsemi_ctx_id_to_ctx_id.itervalues():
                        ctx = self.ctxs[ctx_id]

                        curr_pred_weight = ctx.c1 / float(ctx.c0) if ctx.c0 > 0 else 0.0

                        if curr_pred_weight > max_pred_weight:
                            max_pred_weight = curr_pred_weight
                            prediction_ctxs = [ctx]

                        elif curr_pred_weight == max_pred_weight:
                            prediction_ctxs.append(ctx)

        semi.crossed_semi_ctxs = new_crossed_semi_ctxs

        if left_or_right:
            return self._update_ctxs_and_get_active(new_ctx_flag)
        else:
            [new_predictions.update(ctx.right_facts) for ctx in prediction_ctxs]

            return num_new_ctxs, new_predictions

    def _update_ctxs_and_get_active(self, new_ctx_flag):
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

        @return potential_new_ctxs:     list of contexts based on intersection
                                        between the left and the right zero-level
                                        semi-contexts, which are potentially new
                                        contexts requiring saving to the context
                                        memory
        """

        active_ctxs = []
        num_selected_ctx = 0

        potential_new_ctxs = []

        for lsemi_ctx in self.left.crossed_semi_ctxs:
            for rsemi_ctx_id, ctx_id in lsemi_ctx.rsemi_ctx_id_to_ctx_id.iteritems():

                if self.new_ctx_id != ctx_id:
                    ctx = self.ctxs[ctx_id]
                    rsemi_ctx = self.right.semi_ctxs[rsemi_ctx_id]

                    if lsemi_ctx.init_nfacts == len(lsemi_ctx.facts):
                        num_selected_ctx += 1
                        ctx.c0 += rsemi_ctx.init_nfacts

                        if len(rsemi_ctx.facts) > 0:
                            ctx.c1 += len(rsemi_ctx.facts)

                            if rsemi_ctx.init_nfacts == len(rsemi_ctx.facts):
                                ctx.c2 += 1
                                active_ctxs.append(ActiveCtx(ctx_id, ctx.c2))

                            elif ctx.zerolevel and new_ctx_flag and len(lsemi_ctx.facts) <= self.max_lsemi_ctxs_len:
                                potential_new_ctxs.append((tuple(lsemi_ctx.facts), tuple(rsemi_ctx.facts)))

                    elif ctx.zerolevel and new_ctx_flag and len(rsemi_ctx.facts) > 0 and len(lsemi_ctx.facts) <= self.max_lsemi_ctxs_len:
                        potential_new_ctxs.append((tuple(lsemi_ctx.facts), tuple(rsemi_ctx.facts)))

        self.new_ctx_id = False

        return active_ctxs, num_selected_ctx, potential_new_ctxs
