


let random_int ~min ~max =
  let range = max - min + 1 in
  Random.int range + min

let gaussian ~mu ~sigma =
  let rec sample () =
    let u1 = Random.float 1.0 in
    let u2 = Random.float 1.0 in
    let z0 = sqrt (-2.0 *. log u1) *. cos (2.0 *. Float.pi *. u2) in
    let result = mu +. sigma *. z0 in
    if result < 0.0 || result > 100.0 then sample () else result
  in
  sample