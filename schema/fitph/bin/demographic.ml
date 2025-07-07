open Types  

let participant = {
  name = "participant";
  columns = [
    { name = "id"; typ = Varchar 10; nullable = false };
    { name = "diagnosis"; typ = Varchar 100; nullable = true };
    { name = "dob"; typ = Timestamp; nullable = true };
    { name = "height_m"; typ = Numeric (5,2); nullable = true };
  ];
  constraints = [
    PrimaryKey ["id"];
  ];
}
let weight = {
  name = "weight";
  columns = [
    { name = "id"; typ = Serial; nullable = false };
    { name = "record_date"; typ=Timestamp; nullable = false };  
    { name = "participant_id"; typ = Varchar 10; nullable = false };
    { name = "weight_kg"; typ = Numeric (5,2); nullable = false };
  ];
  constraints = [
    PrimaryKey ["id"];
    ForeignKey {
      columns = ["participant_id"];
      ref_table = "participant";
      ref_columns = ["id"];
    };
  ];
}
let comorbidity = {
  name = "comorbidity";
  columns = [
    { name = "id"; typ = Serial; nullable = false };
    { name = "participant_id"; typ = Varchar 10; nullable = false };
    { name = "comorbidity"; typ = Varchar 100; nullable = false };
  ];
  constraints = [
    PrimaryKey ["id"];
    ForeignKey {
      columns = ["participant_id"];
      ref_table = "participant";
      ref_columns = ["id"];
    };
  ];
}

let demography_tables = [participant; weight; comorbidity]