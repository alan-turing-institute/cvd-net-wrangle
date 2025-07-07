open Types

let string_of_sql_type (atype:sql_type): string = 
  match atype with
  | Serial -> "SERIAL"
  | Int -> "INTEGER"
  | Varchar n -> Printf.sprintf "VARCHAR(%i)" n
  | Text -> "TEXT"
  | Bool -> "BOOLEAN"
  | Timestamp -> "TIMESTAMP"
  | Numeric (dg,dc) -> Printf.sprintf "NUMERIC(%i,%i)" dg dc

let string_of_column (col:column) : string = 
  let base = Printf.sprintf "%s %s" col.name (string_of_sql_type col.typ) in
  let nullable = if col.nullable then "" else " NOT NULL" in
  base ^ nullable 

let string_of_constraint = function
| PrimaryKey cols ->
    Printf.sprintf "PRIMARY KEY (%s)" (String.concat ", " cols)
| ForeignKey { columns; ref_table; ref_columns } ->
    Printf.sprintf
      "FOREIGN KEY (%s) REFERENCES %s(%s)"
      (String.concat ", " columns)
      ref_table
      (String.concat ", " ref_columns)

let generate_create_table_sql tbl =
  let cols_sql = List.map string_of_column tbl.columns in
  let constraints_sql = List.map string_of_constraint tbl.constraints in
  let all_lines = cols_sql @ constraints_sql in
  Printf.sprintf "CREATE TABLE %s (\n  %s\n);" tbl.name (String.concat ",\n  " all_lines)

let generate_insert_into_table_sql (ins:insert):string =
  let values_list = List.map (fun v -> Printf.sprintf "(%s)" @@ "'" ^ v ^"'") ins.values in
  Printf.sprintf "INSERT INTO %s (%s) VALUES %s;" ins.table ins.column @@ String.concat "," values_list 

(* let generate_create_table_sql tbl =
  let cols_sql = List.map string_of_column tbl.columns in
  Printf.sprintf "CREATE TABLE %s (\n  %s\n);" tbl.name (String.concat ",\n  " cols_sql) *)

(* let generate_create_table_composite_sql (tbl:table) : string =
  let cols_sql = List.map string_of_column tbl.columns in
  let pk_sql =
    match tbl.primary_key with
    | [] -> []
    | pk_cols -> [Printf.sprintf "PRIMARY KEY (%s)" (String.concat ", " pk_cols)] 
  in
  let all_lines = cols_sql @ pk_sql in
  Printf.sprintf "CREATE TABLE %s (\n  %s\n);" tbl.name (String.concat ",\n  " all_lines) *)