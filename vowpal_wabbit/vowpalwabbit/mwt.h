/*
Copyright (c) by respective owners including Yahoo!, Microsoft, and
individual contributors. All rights reserved.  Released under a BSD
license as described in the file LICENSE.
 */
#pragma once
LEARNER::base_learner* mwt_setup(vw& all);

namespace MWT
{
void delete_scalars(void* v);
void print_scalars(int f, v_array<float>& scalars, v_array<char>& tag);
}
