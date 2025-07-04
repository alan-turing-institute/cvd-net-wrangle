open Types
open Utilities

(* let participant_table:table = {
  name = "participant";
  columns = [
    { name = "id"; typ = Serial; nullable = false; primary_key = true };
    { name = "height"; typ = Int; nullable = true; primary_key = false };
    { name = "weight"; typ = Int; nullable = true; primary_key = false };
    { name = "age_at_diagnosis"; typ = Timestamp; nullable = true; primary_key = false };
    { name = "diagnosis"; typ = Varchar 255; nullable = true; primary_key = false };
  ];
}

let comorbidities_table:table = {
  name = "comorbidities";
  columns = [
    { name = "id"; typ = Serial; nullable = false; primary_key = true };
    { name = "participant_id"; typ = Serial; nullable = false; primary_key = true };
    
    { name = "diagnosis"; typ = Varchar 255; nullable = true; primary_key = false };
  ];
} *)

let participant_table = {
  name = "participants";
  columns = [
    { name = "participant_id"; typ = Varchar 10; nullable = false };
    { name = "diagnosis"; typ = Varchar 100; nullable = true };
    { name = "dob"; typ = Timestamp; nullable = true };
    { name = "weight"; typ = Numeric (2,2); nullable = true };
  ];
  constraints = [
    PrimaryKey ["id"];
  ];
}

let comorbidity_table = {
  name = "comorbidity";
  columns = [
    { name = "id"; typ = Serial; nullable = false };
    { name = "student_id"; typ = Int; nullable = false };
    { name = "enrolled_on"; typ = Timestamp; nullable = false };
  ];
  constraints = [
    PrimaryKey ["id"];
    ForeignKey {
      columns = ["student_id"];
      ref_table = "students";
      ref_columns = ["id"];
    };
  ];
}

let () =
  (* let participant_sql = generate_create_table_sql participant_table in
  let comorbidities_sql = generate_create_table_sql comorbidities_table in
  print_endline @@ Printf.sprintf "%s\n%s" participant_sql comorbidities_sql*)
  print_endline (generate_create_table_sql participant_table);
  print_endline "";
  print_endline (generate_create_table_sql enrollment_table) 