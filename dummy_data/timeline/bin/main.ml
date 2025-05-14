(* open Types *)
open Utilities
open Population


let () =
  let statistics = yaml_to_hashtbl (Fpath.v  "bin/statistics.yaml") in
  
  (* Hashtbl.iter (fun k v -> Printf.printf "%s -> %d\n" k v) statistics; *)
  let patients = generate_population 5 statistics in
  List.iter print_patient patients;
 