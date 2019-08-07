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

import collections

import recordclass

Half = recordclass.recordclass('Half', [
        'fact_to_semi_ctx',  # fact => semi ctx
        'facts_hash_to_semi_ctx_id',  # facts hash => semi ctx ids (seq ints)
        'semi_ctxs',  # semi ctx id => semi ctx
        'crossed_semi_ctxs',  # subset of semi_ctxs with len(.facts) > 0
])

Ctx = recordclass.recordclass('Ctx', [
        'c0',
        'c1',
        'num_activations',
        'right_facts',
        'zerolevel',
])

SemiCtx = recordclass.recordclass('SemiCtx', [
        'facts',
        'init_nfacts',
        'rsemi_ctx_id_to_ctx_id',
])

ActiveCtx = collections.namedtuple('ActiveCtx', [
        'ctx_id',
        'ctx_num_activations',
])


def _prepare_crossed_semi_ctxs(half, facts):
    # Erase cross semi contexts
    for semi_ctx in half.crossed_semi_ctxs:
        semi_ctx.facts = []

    # For every fact append it to every semi contexts fact attribute
    for fact in facts:
        for semi_ctx in half.fact_to_semi_ctx.get(fact, []):
            semi_ctx.facts.append(fact)

    # Set the crossed semi contexts to all the semi contexts in the half
    # if it exists
    half.crossed_semi_ctxs = [semi_ctx for semi_ctx in half.semi_ctxs
                              if semi_ctx.facts]


class ContextOperator(object):
    """
    TODO Write a docstring for this one
    """
    def __init__(self, max_lsemi_ctxs_len):
        self.max_lsemi_ctxs_len = max_lsemi_ctxs_len

        # Initialize both halves attributes to be empty
        self.left = Half({}, {}, [], [])
        self.right = Half({}, {}, [], [])
        self.ctxs = []

        # Set the new context ID to be false
        self.new_ctx_id = False

    def cross_ctxs_right(self, facts, pot_new_zero_level_ctx):
        """
        TODO: Write a docstring that describes the crazy logic below in a
              significantly more compact and logical manner.
        :param facts: The facts
        :param pot_new_zero_level_ctx: The potential new zero level contexts
        :return:
        """
        # Reset the crossed semi contexts for the right half
        _prepare_crossed_semi_ctxs(self.right, facts)

        # Get the number of new contexts
        num_new_ctxs = self._add_ctxs_by_facts(pot_new_zero_level_ctx,
                                               zerolevel=True)

        # Initialize some variables
        active_ctxs = []
        num_selected_ctx = 0
        potential_new_ctxs = []

        # Loop through the left semi contexts
        for lsemi_ctx in self.left.crossed_semi_ctxs:
            # Loop through the right semi context ID's
            # TODO: Update the deprecated iteritems() call
            for rsemi_ctx_id, ctx_id in lsemi_ctx.rsemi_ctx_id_to_ctx_id.items():
                # If context ID does not equal the new context ID set the
                # context to the current context ID, and the right semi context
                # to the right semi context with ID right semi context ID
                if ctx_id != self.new_ctx_id:
                    ctx = self.ctxs[ctx_id]
                    rsemi_ctx = self.right.semi_ctxs[rsemi_ctx_id]
                    # If the length of the left semi context facts is equal
                    # to the left semi context initial number of facts
                    # increment the number of selected contexts, increment
                    # the context 0 by the right semi context initial number
                    # of faces, and the context 1 by the length of right semi
                    # context facts
                    if len(lsemi_ctx.facts) == lsemi_ctx.init_nfacts:
                        num_selected_ctx += 1
                        ctx.c0 += rsemi_ctx.init_nfacts
                        ctx.c1 += len(rsemi_ctx.facts)
                        # If the right semi contexts facts are equal to the
                        # initial facts increment the contexts number of
                        # activations by 1, and append a 'recordclass'
                        # ActiveCtx to the active contexts, with context ID and
                        # the contexts number of activations
                        if len(rsemi_ctx.facts) == rsemi_ctx.init_nfacts:
                            ctx.num_activations += 1
                            active_ctxs.append(
                                ActiveCtx(ctx_id, ctx.num_activations))
                        # If the above is not true check if we are on the
                        # contexts zero level, if the number of new contexts
                        # is not zero, if the right semi contexts facts is
                        # not 0 and the number left semi contexts facts are
                        # leq to the maximum left semi contexts length
                        elif ctx.zerolevel and num_new_ctxs and rsemi_ctx.facts and len(
                                lsemi_ctx.facts) <= self.max_lsemi_ctxs_len:
                            # Append the left semi context facts to the right
                            # semi context facts
                            potential_new_ctxs.append((tuple(lsemi_ctx.facts),
                                                       tuple(rsemi_ctx.facts)))
                    # If the the length of the left semi context facts is not
                    # equal to the initial number of facts, check to see if
                    # the context is at zero level, and the number of new
                    # contexts is not 0, the right semi context facts is not 0,
                    # and the length of the left semi context facts is leq than
                    # the max left semi context length
                    # TODO: Note this is a duplicate above the elif directly
                    #       above this, why is that?
                    elif ctx.zerolevel and num_new_ctxs and rsemi_ctx.facts and len(
                            lsemi_ctx.facts) <= self.max_lsemi_ctxs_len:
                        potential_new_ctxs.append((tuple(lsemi_ctx.facts),
                                                   tuple(rsemi_ctx.facts)))

        # Set the new context ID to be false
        self.new_ctx_id = False

        return active_ctxs, num_selected_ctx, potential_new_ctxs, num_new_ctxs

    def cross_ctxs_left(self, facts, potential_new_ctxs):
        # Reset the crossed semi contexts
        _prepare_crossed_semi_ctxs(self.left, facts)

        # Get the number of new contexts
        num_new_ctxs = self._add_ctxs_by_facts(potential_new_ctxs,
                                               zerolevel=False)
        # TODO: Investigate why this was cut from the code for 'patent' reasons
        max_pred_weight = 0.0
        prediction_ctxs = []

        # Iterate over the left semi contexts
        for lsemi_ctx in self.left.semi_ctxs:
            # Check to see if both the length of the left semi contexts and
            # the length of the left semi contexts initial number of facts
            # are equal and that they are greater than 0
            if 0 < len(lsemi_ctx.facts) == lsemi_ctx.init_nfacts:
                # Loop over context ID's in the left semi context's
                for ctx_id in lsemi_ctx.rsemi_ctx_id_to_ctx_id.values():
                    # Set the context based off the context ID
                    ctx = self.ctxs[ctx_id]
                    # Calculate the current prediction weight given ctx.c0 > 0
                    curr_pred_weight = ctx.c1 / float(
                        ctx.c0) if ctx.c0 > 0 else 0.0

                    # If the current prediction weight is larger than the max
                    # prediction weight, set max to current, and also say the
                    # prediction context is the list of the context
                    if curr_pred_weight > max_pred_weight:
                        max_pred_weight = curr_pred_weight
                        prediction_ctxs = [ctx]
                    # If the current prediction weight is equal to the max
                    # append the context to the prediction contexts
                    elif curr_pred_weight == max_pred_weight:
                        prediction_ctxs.append(ctx)

        # Create a set of new predictions (which are facts) that loops through
        # all the prediction contexts and every right side fact in that context
        new_predictions = set(
                fact for ctx in prediction_ctxs for fact in ctx.right_facts)

        return num_new_ctxs, new_predictions

    def _add_ctxs_by_facts(self, new_ctxs, zerolevel):
        num_added_ctxs = 0

        for left_facts, right_facts in new_ctxs:
            lsemi_ctx_id = self._add_semi_ctx_by_facts(self.left, left_facts)
            rsemi_ctx_id = self._add_semi_ctx_by_facts(self.right, right_facts)

            next_free_ctx_id_number = len(self.ctxs)
            ctx_id = self.left.semi_ctxs[
                lsemi_ctx_id].rsemi_ctx_id_to_ctx_id.setdefault(rsemi_ctx_id,
                                                                next_free_ctx_id_number)

            if ctx_id == next_free_ctx_id_number:
                ctx = Ctx(0, 0, 0, right_facts, zerolevel)
                self.ctxs.append(ctx)
                num_added_ctxs += 1
                if zerolevel:
                    self.new_ctx_id = ctx_id
            else:
                ctx = self.ctxs[ctx_id]
                if zerolevel:
                    ctx.zerolevel = True

        return num_added_ctxs

    def _add_semi_ctx_by_facts(self, half, facts):
        next_semi_ctx_number = len(half.facts_hash_to_semi_ctx_id)
        semi_ctx_id = half.facts_hash_to_semi_ctx_id.setdefault(hash(facts),
                                                                next_semi_ctx_number)
        if semi_ctx_id == next_semi_ctx_number:
            semi_ctx = SemiCtx([], len(facts),
                               {} if half == self.left else None)
            half.semi_ctxs.append(semi_ctx)
            for fact in facts:
                semi_ctxs = half.fact_to_semi_ctx.setdefault(fact, [])
                semi_ctxs.append(semi_ctx)
        return semi_ctx_id
