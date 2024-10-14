Delaware North: Take-Home Assignment

This project includes ‘main.py’, which is the initial scaffolding for your solution.
The `ttpd_data.zip’ file is your dataset.
This directory, once unzipped, will serve as your source file dataset that will need to be ingested and analyzed.
Make sure to utilize the Spark DataFrame API for the core of your solution; beyond that, you may use additional Python libraries as you see fit.
If you are using a Windows machine for this assignment, you may have some issues setting up Spark. If so, feel free to use this docker image to set up a local environment: https://hub.docker.com/r/jupyter/all-spark-notebook/.
Please save all code, results, and documentation to a git repository (e.g., GitHub, Bitbucket) that is available for us to access and review prior to our follow-up call to discuss your solution.
Feel free to reach out with any questions or issues if any arise.
The overall timeline for this assignment is 2-3 days, or 8 hours of work total.

1) Clone the repository locally.
2) Create a feature branch off main.
3) Submit a PR with comments about your update, along with any screenshots or outputs that would be helpful in the PR for testing.

Technical Prerequisites:
  - Python 3+
  - Spark: pip install delta-spark
  - Java Runtime: https://java.com/en/download/manual.jsp

Assignment Background:
    - You are a freelance analytics consultant who has partnered with the TTPD (Tiny Town Police Department)
      to analyze speeding tickets that have been given to the adult citizens of Tiny Town over the 2020-2023 period.
    - Inside the folder "ttpd_data" you will find a directory of data for Tiny Town. This dataset will need to be "ingested" for analysis.
    - The solutions must use the Dataframes API.
    - You will need to ingest this data into a PySpark environment and answer the following three questions for the TTPD.

Questions:
    1. Which police officer was handed the most speeding tickets?
        - Police officers are recorded as citizens. Find in the data what differentiates an officer from a non-officer.
    2. What 3 months (year + month) had the most speeding tickets? 
        - Bonus: What overall month-by-month or year-by-year trends, if any, do you see?
    3. Using the ticket fee table below, who are the top 10 people who have spent the most money paying speeding tickets overall?

Ticket Fee Table:
    - Ticket (base): $30
    - Ticket (base + school zone): $60
    - Ticket (base + construction work zone): $60
    - Ticket (base + school zone + construction work zone): $120

## ANSWERS:
1. I think the wording may be off. joining on `id <-> id` yielded no results, however joining `id <-> officer_id` did work. I think we were looking for the officer that gave out the most tickets, not an officer who received the most tickets. If my assumption is correct, the officer with the most tickets is `Barbara Cervantes`. I saved the the results as a table, however, if we want to be really granular and ONLY want the top ticketer we could slap on a `df.first()`
2. Started with a `groupBy(F.window("ticket_time","12 weeks"))` but I didn't like how uneven the dates were distributed because of months not having a equal amount of days/weeks. Unfortunately, `"3 months"` isn't valid because of this error I got `Intervals greater than a month is not supported (3 months).`. So I decided to go with a solution that divides up the the tickets by year+quarter, which would be the way most MBA types would probably want it anyway. The months with the most tickets would be `Q3 (Jul,Aug,Sep) of 2023`.
2. (bonus) When we break it down to year-to-year, theres a large jump in 2023 and 2020 is a very light year. Looking at the months, December of 23 saw a significant increase (maybe they had to make a quota...). 2020 was relatively stable while being low, so maybe there was an increase in hires that led to more ticket revenue or something.
3. Since we previously determined id in the ticket table is likely a uuid for the ticket itself (and can be confirmed by doing a unique or groupby), the next best thing would be the license plate. It's not a perfect sub for a person since people can drive multiple cars, but you gotta work with what you got. With that, the top paid was `HTR 019`, and the rest were saved via delta lake. I'll add a commented block at the bottom for shows on all the dataframes so you can check the work quickly.