open Types
let sex_table = {
  name = "sex";
  columns = [
    { name = "id"; typ = Serial; nullable = false };
    { name = "category"; typ = Varchar 6; nullable = true };
  ];
  constraints = [
    PrimaryKey ["id"];
  ];
}

let sex_vocab = {
  table = "sex";
  column= "category";
  values=["Female";"Male"]
}

let vocabulary_tables = [sex_table]
let vocabulary_values = [sex_vocab]
