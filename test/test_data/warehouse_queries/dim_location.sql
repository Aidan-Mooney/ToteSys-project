DROP TABLE IF EXISTS dim_location;
CREATE TABLE dim_location (
    location_id INT,
    address_line_1 VARCHAR(100),
    address_line_2 VARCHAR(100),
    district VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(100),
    country VARCHAR(100),
    phone VARCHAR(100),
);
INSERT INTO dim_location
    (location_id, address_line_1, address_line_2, district, city, postal_code, country, phone)
VALUES
    (1, 6826 Herzog Via, NULL, Avon, New Patienceburgh, 28441, Turkey, 1803 637401)
    (2, 179 Alexie Cliffs, NULL, NULL, Aliso Viejo, 99305-7380, San Marino, 9621 880720)
    (3, 148 Sincere Fort, NULL, NULL, Lake Charles, 89360, Samoa, 0730 783349)
    (4, 6102 Rogahn Skyway, NULL, Bedfordshire, Olsonside, 47518, Republic of Korea, 1239 706295)
    (5, 34177 Upton Track, NULL, NULL, Fort Shadburgh, 55993-8850, Bosnia and Herzegovina, 0081 009772)
    (6, 846 Kailey Island, NULL, NULL, Kendraburgh, 08841, Zimbabwe, 0447 798320)
    (7, 75653 Ernestine Ways, NULL, Buckinghamshire, North Deshaun, 02813, Faroe Islands, 1373 796260)
    (8, 0579 Durgan Common, NULL, NULL, Suffolk, 56693-0660, United Kingdom, 8935 157571)
    (9, 644 Edward Garden, NULL, Borders, New Tyra, 30825-5672, Australia, 0768 748652)
    (10, 49967 Kaylah Flat, Tremaine Circles, Bedfordshire, Beaulahcester, 89470, Democratic People''s Republic of Korea, 4949 998070)
    (11, 249 Bernier Mission, NULL, Buckinghamshire, Corpus Christi, 85111-9300, Japan, 0222 525870)
    (12, 6461 Ernesto Expressway, NULL, Berkshire, Pricetown, 37167-0340, Tajikistan, 4757 757948)
    (13, 80828 Arch Dale, Torphy Turnpike, NULL, Shanahanview, 60728-5019, Bouvet Island (Bouvetoya), 8806 209655)
    (14, 84824 Bryce Common, Grady Turnpike, NULL, Maggiofurt, 50899-1522, Iraq, 3316 955887)
    (15, 605 Haskell Trafficway, Axel Freeway, NULL, East Bobbie, 88253-4257, Heard Island and McDonald Islands, 9687 937447)
    (16, 511 Orin Extension, Cielo Radial, Buckinghamshire, South Wyatt, 04524-5341, Iceland, 2372 180716)
    (17, 962 Koch Drives, NULL, NULL, Hackensack, 95316-4738, Indonesia, 5507 549583)
    (18, 58805 Sibyl Cliff, Leuschke Glens, Bedfordshire, Lake Arne, 63808, Kiribati, 0168 407254)
    (19, 0283 Cole Corner, Izabella Ways, Buckinghamshire, West Briellecester, 01138, Sierra Leone, 1753 158314)
    (20, 22073 Klein Landing, NULL, NULL, Pueblo, 91445, Republic of Korea, 4003 678621)
    (21, 389 Georgette Ridge, NULL, Cambridgeshire, Fresno, 91510-3655, Bolivia, 8697 474676)
    (22, 364 Goodwin Streets, NULL, NULL, Sayreville, 85544-4254, Svalbard & Jan Mayen Islands, 0847 468066)
    (23, 822 Providenci Spring, NULL, Berkshire, Derekport, 25541, Papua New Guinea, 4111 801405)
    (24, 8434 Daren Freeway, NULL, NULL, New Torrance, 17110, Antigua and Barbuda, 5582 055380)
    (25, 253 Ullrich Inlet, Macey Wall, Borders, East Arvel, 35397-9952, Sudan, 0021 366201)
    (26, 522 Pacocha Branch, NULL, Bedfordshire, Napa, 77211-4519, American Samoa, 5794 359212)
    (27, 7212 Breitenberg View, Nora Bridge, Buckinghamshire, Oakland Park, 77499, Guam, 2949 665163)
    (28, 079 Horacio Landing, NULL, NULL, Utica, 93045, Austria, 7772 084705)
    (29, 37736 Heathcote Lock, Noemy Pines, NULL, Bartellview, 42400-5199, Congo, 1684 702261)
    (30, 0336 Ruthe Heights, NULL, Buckinghamshire, Lake Myrlfurt, 94545-4284, Falkland Islands (Malvinas), 1083 286132)
;