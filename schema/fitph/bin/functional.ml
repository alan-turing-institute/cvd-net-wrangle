open Types

let sixmwd = {
  name = "6_min_walk_distance";
  columns = [
    { name = "id"; typ = Serial; nullable = false };
    { name = "6mwd_m"; typ=Int; nullable = true };  
    { name = "visit_id"; typ=Int; nullable = false };
    { name = "notes"; typ=Varchar 100; nullable = true };
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

let functional_test_tables = [sixmwd]