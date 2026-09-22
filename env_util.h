/*
 * env_util.h
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Environment-variable tunables, parsed the same way everywhere: an unset or
 * empty variable yields the default silently; anything unparsable or out of
 * range is reported once on stderr and replaced by the default or the nearest
 * bound. Shared by ecopy and edelete.
 */

#ifndef ENV_UTIL_H
#define ENV_UTIL_H

/* Integer in [minval, maxval]; out-of-range values are clamped with a warning. */
int env_int_or_default(const char *name, int defval, int minval, int maxval);

/*
 * Boolean switch: true when the variable is set, non-empty, and not "0".
 * Used for DIRECT_COPY_DISABLE_* style knobs.
 */
int env_flag_set(const char *name);

#endif /* ENV_UTIL_H */
