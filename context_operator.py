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

import collections
import recordclass


Half = recordclass.recordclass('Half', [
    'fact_to_semi_ctx',             # fact => semi ctx (which was created with it)
    'facts_hash_to_semi_ctx_id',    # facts hash => semi ctx ids (sequential integers)
    'semi_ctxs',                    # semi ctx id => semi ctx
    'crossed_semi_ctxs',            # subset of semi_ctxs with len(.facts) > 0
])

Ctx = recordclass.recordclass('Ctx', [
    'c0',
    'c1',
    'c2',
    'right_facts',
    'zerolevel',
    'left_hash',
    'right_hash',
])

SemiCtx = recordclass.recordclass('SemiCtx', [
    'facts',
    'init_nfacts',
    'rsemi_ctx_id_to_ctx_id',
])

ActiveCtx = collections.namedtuple('ActiveCtx', [
    'ctx_id',
    'ctx_c2',
])


def _prepare_crossed_semi_ctxs(semi, facts):
    for semi_ctx in semi.crossed_semi_ctxs:
        semi_ctx.facts = []

    for fact in facts:
        for semi_ctx in semi.fact_to_semi_ctx.get(fact, []):
            semi_ctx.facts.append(fact)

    semi.crossed_semi_ctxs = [semi_ctx for semi_ctx in semi.semi_ctxs if len(semi_ctx.facts) > 0]


class ContextOperator(object):
    def __init__(self, max_lsemi_ctxs_len):
        self.max_lsemi_ctxs_len = max_lsemi_ctxs_len

        self.left = Half({}, {}, [], [])
        self.right = Half({}, {}, [], [])
        self.ctxs = []

        self.new_ctx_id = False

    def cross_ctxs_right(self, facts, pot_new_zero_level_ctx):
        _prepare_crossed_semi_ctxs(self.right, facts)

        new_ctx_flag = self._get_ctx_by_facts(pot_new_zero_level_ctx, zerolevel=True)
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

        return active_ctxs, num_selected_ctx, potential_new_ctxs, new_ctx_flag

    def cross_ctxs_left(self, facts, potential_new_ctxs):
        _prepare_crossed_semi_ctxs(self.left, facts)

        num_new_ctxs = self._get_ctx_by_facts(potential_new_ctxs, zerolevel=False)
        max_pred_weight = 0.0
        new_predictions = set()
        prediction_ctxs = []

        for semi_ctx in self.left.semi_ctxs:
            if 0 < len(semi_ctx.facts) == semi_ctx.init_nfacts:
                for ctx_id in semi_ctx.rsemi_ctx_id_to_ctx_id.itervalues():
                    ctx = self.ctxs[ctx_id]

                    curr_pred_weight = ctx.c1 / float(ctx.c0) if ctx.c0 > 0 else 0.0

                    if curr_pred_weight > max_pred_weight:
                        max_pred_weight = curr_pred_weight
                        prediction_ctxs = [ctx]

                    elif curr_pred_weight == max_pred_weight:
                        prediction_ctxs.append(ctx)

        for ctx in prediction_ctxs:
            new_predictions.update(ctx.right_facts)

        return num_new_ctxs, new_predictions

    def _get_ctx_by_facts(self, new_ctxs, zerolevel):
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
                    ctx.zerolevel = True
                    return False

        return num_added_ctxs

