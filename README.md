# DaoParser - a minimalistic microORM library.

Many years ago, our team had a project with a lot of stored procedures returning multiple resultsets.
We wrote by hand "DAO" classes that would convert those resultsets into object graphs.
It was very time consuming and error-prone, so one evening I wrote this little library at home,
than brought it to work and used it for that project. It worked pretty well (sometimes faster than the hand-written DAOs)
and avoided some errors the hand-written DAOs sometimes had (e.g., WRT nullability).

(Why would it work faster? I think because the hand-written code would use strings to access columns, and this library optimizes that away)

I haven't touch the code for years so this library isn't really supported. But I'm kind of proud of it, so this is why I published it.