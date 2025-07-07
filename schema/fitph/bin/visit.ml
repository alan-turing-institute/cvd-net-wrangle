open Types
let visit = {
  name = "visit";
  columns = [
    { name = "id"; typ = Serial; nullable = false };
    { name = "participant_id"; typ = Varchar 10; nullable = false };
    { name = "week"; typ = Int; nullable = false };
    { name = "days_after_anchor"; typ = Int; nullable = true };
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

let visit_label = {
  name = "timeline_label";
  columns = [
    { name = "id"; typ = Serial; nullable = false };
    { name = "label"; typ = Varchar 10; nullable = false };
  ];
  constraints = [
    PrimaryKey ["id"];
  ];
}

let visit_tables = [visit; visit_label] 