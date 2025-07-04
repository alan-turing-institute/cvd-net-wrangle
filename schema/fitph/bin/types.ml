type sql_type =
  | Serial
  | Int
  | Varchar of int
  | Text
  | Bool
  | Timestamp
  | Numeric of int*int

type constraint_ =
  | PrimaryKey of string list
  | ForeignKey of {
      columns : string list;
      ref_table : string;
      ref_columns : string list;
    }

type column = {
  name : string;
  typ : sql_type;
  nullable : bool;
}

type table = {
  name : string;
  columns : column list;
  constraints: constraint_ list;
}