export const Bindings = ({ sql }: { sql: string }) => {
	const { data } = useQuery<Person[]>("SELECT * FROM person");
	return (
		<div>
			<Query q="SELECT * FROM post WHERE age &gt; 18" />
			<LiveQuery q={`SELECT * FROM tag`} />
			<Kit.Query q={"SELECT * FROM note"} />
			<Other q="SELECT * FROM other" />
			<Query q={sql} />
			<Query q="SELECT * FROM log WHERE body = 'a > b'" />
			{/* <Query q="SELECT * FROM commented" /> */}
		</div>
	);
};
