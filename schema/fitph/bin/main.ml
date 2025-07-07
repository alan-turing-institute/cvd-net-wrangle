(* open Types *)
open Utilities
open Demographic
open Medical_history
open Visit
open Clinical_assessment
open Vocabulary


let () =
  let data_tables = demography_tables @ visit_tables @ medical_history_tables @ clinical_assessment_tables in
  
  let oc = open_out "create_schema.sql" in
  List.iter (fun table -> Printf.fprintf oc "\n\n%s" @@ generate_create_table_sql table) vocabulary_tables;
  List.iter (fun values-> Printf.fprintf oc "\n\n%s" @@ generate_insert_into_table_sql values) vocabulary_values;
  List.iter (fun table -> Printf.fprintf oc "\n\n%s" @@ generate_create_table_sql table) data_tables;
  close_out oc;
  (* let file = open_out "create.sql" in
  let sql = List.map (fun table-> Printf.sprintf "\n\n%s" @@ generate_create_table_sql table ) tables in
  Printf.fprintf oc "The answer is: %d\n" 42; *)
