# General Guidelines

1. Ensure that your code both **compiles** and **runs** on the undergraduate servers. Failure to compile successfully will result in a zero for the lab.

2. All test cases carry equal weight in grading. If your program passes 3 out of 10 tests for a lab, you will receive 30% of the grade for that lab. Any runtime errors leading to test failures are considered as **not passed**.
   - There may be an oral examination for each lab. In such cases, the lab grading will be split 50% between automated testing and 50% for the oral examination.
   - We run each test multiple times, and failing any of them will fail that particular test.

3. Stick to the provided Go built-in packages for coding; do not use external packages unless explicitly allowed. Using an external package for a lab will result in a zero for that lab.

4. Each lab provides a basic template to start your work. Ideally, make changes only to the files required. These are sufficient places where you can put your code to get full marks for the lab. However, you can organize your code by adding packages and files, but refrain from changing specified files and folder structures. These specified files and folders are replaced during grading, so organize your code in a way that doesn't depend on changes in them.

5. If you need to make changes to files specified as unchangeable for debugging purposes, feel free to do so. However, ensure that your program ultimately compiles and runs with the original version provided.

# Submitting Your Code

When submitting your code on Canvas:

- Use `make tar lab_name=LAB_NAME student_id=STUDENT_ID` to create a `.tar.gz` version for your lab. The `STUDENT_ID` should adhere to the naming pattern:
  - For lab 1: `i_[stu-number]`
  - For lab 2-4: `g_[stu-number1]_[stu-number2]`
  - Submit the `tar` file on Canvas.

   The following is an example of how to do it (assuming the repository is already cloned in the home directory and lab1 is completed):

```bash
sinaee@pender:~$ ls
cpsc416-2023w2-golabs

sinaee@pender:~$ cd cpsc416-2023w2-golabs

sinaee@pender:~$ ls
Makefile  README.md src docs

sinaee@pender:~$ make tar lab_name=lab1 student_id=i_87654321
# a lot of output; you should not see any errors

sinaee@pender:~$ ls
Makefile          README.md         i_87654321.tar.gz   src     docs
```

Now, upload the `i_87654321.tar.gz` to Canvas. If we cannot untar your file, we cannot evaluate your lab. Therefore, ensure that the `tar` object is created successfully by simply downloading your submission and untarring it. Use the following command to untar your files.

```bash
sinaee@pender:~$ tar xzvf i_87654321.tar.gz
```

# Labs

- See [Lab 1 instructions](docs/lab1.md)
