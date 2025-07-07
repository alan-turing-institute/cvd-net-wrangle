open Types  

let participant = {
  name = "participant";
  columns = [
    { name = "id"; typ = Varchar 10; nullable = false };
    { name = "diagnosis"; typ = Varchar 100; nullable = true };
    { name = "dob"; typ = Timestamp; nullable = true };
    { name = "height_m"; typ = Numeric (5,2); nullable = true };
    { name = "sex_id"; typ = Int; nullable = true };
  ];
  constraints = [
    PrimaryKey ["id"];
    ForeignKey {
      columns = ["sex_id"];
      ref_table = "sex";
      ref_columns = ["id"];
    };
  ];
}

let sex = {
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

let demography_tables = [participant; sex]
let demography_vocabulary = [sex_vocab]