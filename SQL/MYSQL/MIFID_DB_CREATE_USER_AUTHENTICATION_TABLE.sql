use mifid;
DROP TABLE USER_AUTHENTICATION;
CREATE TABLE USER_AUTHENTICATION (
    ENTRY_ID INT NOT NULL AUTO_INCREMENT primary key,
    USER_EMAIL varchar(255)
    TOKEN_ID varchar(255),
    ENTRY_TIMESTAMP timestamp default CURRENT_TIMESTAMP()
);