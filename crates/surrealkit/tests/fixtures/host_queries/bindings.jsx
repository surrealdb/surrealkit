export const Bindings = ({ sql }) => {
	const { data } = useLive("SELECT * FROM person");
	return (
		<div>
			<Query q="SELECT * FROM post" />
			<LiveQuery q={`SELECT * FROM tag WHERE id = ${id}`} />
			<Other q="SELECT * FROM other" />
			<Query q={sql} />
			<Query q="SELECT * FROM log WHERE body = 'a > b'" />
		</div>
	);
};
