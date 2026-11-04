export const Widget = ({ db }: { db: Db }) => {
	const rows = db.query("SELECT * FROM widget");
	return <div>{rows}</div>;
};
