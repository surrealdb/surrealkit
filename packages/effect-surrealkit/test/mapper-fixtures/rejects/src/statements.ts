/** A missing binding fails to compile where the part is used. */
import * as SurrealSchema from 'effect-surrealkit';
import { sequence } from 'effect-surrealkit/statement';

import { bindOwner } from '../queries/accounts/bindOwner.surql';

export const owner = sequence(bindOwner).define({
	params: {},
	result: SurrealSchema.String,
});
