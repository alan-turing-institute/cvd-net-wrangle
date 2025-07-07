open Types
let gad2 = {
  name = "gad2";
  columns = [
    { name = "id"; typ = Serial; nullable = false };
    { name = "score"; typ=Int; nullable = true };  
    { name = "visit_id"; typ=Int; nullable = false };
  ];
  constraints = [
    PrimaryKey ["id"];
    ForeignKey {
      columns = ["visit_id"];
      ref_table = "visit";
      ref_columns = ["id"];
    };
  ];
}

let gad7 = {
  name = "gad7";
  columns = [
    { name = "id"; typ = Serial; nullable = false };
    { name = "score"; typ=Int; nullable = true };  
    { name = "visit_id"; typ=Int; nullable = false };
  ];
  constraints = [
    PrimaryKey ["id"];
    ForeignKey {
      columns = ["visit_id"];
      ref_table = "visit";
      ref_columns = ["id"];
    };
  ];
}

let questionaire_tables = [gad2; gad7]