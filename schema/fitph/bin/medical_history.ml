open Types

let comorbidity = {
  name = "comorbidity";
  columns = [
    { name = "id"; typ = Serial; nullable = false };
    { name = "visit_id"; typ = Int; nullable = false };
    { name = "comorbidity"; typ = Varchar 100; nullable = false };
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

let medical_history_tables = [comorbidity]