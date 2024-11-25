DROP TABLE IF EXISTS dim_staff;
CREATE TABLE dim_staff (
    staff_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email_address VARCHAR(100),
    department_name VARCHAR(100),
    location VARCHAR(100),
);
INSERT INTO dim_staff
    (staff_id, first_name, last_name, email_address, department_name, location)
VALUES
    (1, Jeremie, Franey, jeremie.franey@terrifictotes.com, Purchasing, Manchester)
    (2, Deron, Beier, deron.beier@terrifictotes.com, Facilities, Manchester)
    (3, Jeanette, Erdman, jeanette.erdman@terrifictotes.com, Facilities, Manchester)
    (4, Ana, Glover, ana.glover@terrifictotes.com, Production, Leeds)
    (5, Magdalena, Zieme, magdalena.zieme@terrifictotes.com, HR, Leeds)
    (6, Korey, Kreiger, korey.kreiger@terrifictotes.com, Production, Leeds)
    (7, Raphael, Rippin, raphael.rippin@terrifictotes.com, Purchasing, Manchester)
    (8, Oswaldo, Bergstrom, oswaldo.bergstrom@terrifictotes.com, Communications, Leeds)
    (9, Brody, Ratke, brody.ratke@terrifictotes.com, Purchasing, Manchester)
    (10, Jazmyn, Kuhn, jazmyn.kuhn@terrifictotes.com, Purchasing, Manchester)
    (11, Meda, Cremin, meda.cremin@terrifictotes.com, Finance, Manchester)
    (12, Imani, Walker, imani.walker@terrifictotes.com, Finance, Manchester)
    (13, Stan, Lehner, stan.lehner@terrifictotes.com, Dispatch, Leds)
    (14, Rigoberto, VonRueden, rigoberto.vonrueden@terrifictotes.com, Communications, Leeds)
    (15, Tom, Gutkowski, tom.gutkowski@terrifictotes.com, Production, Leeds)
    (16, Jett, Parisian, jett.parisian@terrifictotes.com, Facilities, Manchester)
    (17, Irving, O''Keefe, irving.o''keefe@terrifictotes.com, Production, Leeds)
    (18, Tomasa, Moore, tomasa.moore@terrifictotes.com, HR, Leeds)
    (19, Pierre, Sauer, pierre.sauer@terrifictotes.com, Purchasing, Manchester)
    (20, Flavio, Kulas, flavio.kulas@terrifictotes.com, Production, Leeds)
;