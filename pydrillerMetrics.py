from dateutil.relativedelta import *
from pydriller import RepositoryMining,GitRepository
import re
import csv
from multiprocessing.dummy import Pool as ThreadPool

# To understand this work, move to the function main and start from there reading the comments
repoPathList = ["/Users/gakuo/PycharmProjects/pydriller/githubProjects/aries",
                 "/Users/gakuo/PycharmProjects/pydriller/githubProjects/falcon",
                "/Users/gakuo/PycharmProjects/pydriller/githubProjects/ranger",
                "/Users/gakuo/PycharmProjects/pydriller/githubProjects/sqoop",
                "/Users/gakuo/PycharmProjects/pydriller/githubProjects/attic-whirr"
                ]
def computeRepoMetrics(repoPath):
    print("STARTED:" + repoPath)
    gr = GitRepository(repoPath)
    #for each repo calculate repo metrics in parallel
    # calculateStructAndSemanticScattering =>
    #            we finally return file pairs and their related semantic
    #           and structural differences for each two pairs of files for the entire repo
    # analyzeCommits =>
    #             collects other related metrics for the interval
    parallelMetricProcessing(analyzeCommits(repoPath,gr), calculateStructAndSemanticScattering(gr), 30,gr)
    print("COMPLETED:" + repoPath)


def calculateChangeBurst(intervalhashes_list, filehashesSet):
    filehashes = list(filehashesSet)
    matchedLengths = [0]
    matched = 0
    for index in range(len(filehashes)):
        slice = intervalhashes_list[intervalhashes_list.index(filehashes[index]):]
        for index in range(len(slice)):
            if (index < len(filehashes)):
                if slice[index] == filehashes[index]:
                    matched = matched + 1
                    matchedLengths.append(matched)
                else:
                    matched = 0
            else:
                matched = 0

    return max(matchedLengths)


def getPastFaults(commit,gr):
    return gr.get_commits_last_modified_lines(commit)


def analyzePastFaults(repoPath,gr): # What are the components? File? Methods, Class
    #returns a tuple of bug fixing commits and bug introducing commits
    buggyCommits = set()
    faultFixingCommits = []
    for commit in RepositoryMining(repoPath,only_modifications_with_file_types=['.java']).traverse_commits(): # specify java
            if regularExpFinder(commit.msg):
                faultFixingCommits.append(commit.hash)
                #print(commit.msg)
                buggyCommits.update(getPastFaults(commit,gr))
                #print(getPastFaults(commit))
    return (buggyCommits,faultFixingCommits)


def computeNumberOfFaultsPerPeriod(intervals_data,buggy_commits):
    allFaultyCommits = []
    for intervalData in intervals_data:
        fileIntervalBuggyCommits = {}
        for filename_name in intervalData[1]:
            buggyCommits = 0
            for commit_hash in intervalData[1][filename_name]:
                if commit_hash in buggy_commits:
                    buggyCommits +=1
            fileIntervalBuggyCommits[filename_name] = buggyCommits
        allFaultyCommits.append(fileIntervalBuggyCommits)
    return allFaultyCommits


def calculateStructAndSemanticScattering(gr):
    # hThe steps here work by getting the filename and breaking it up into its nested directories.
    # We then compute the number of steps between each two file pairs from a common root folder
    java_files = filter((lambda x:  (re.search(r'\.java',x))),gr.files())
    # this stores directory name and a list of all directories in the path to this file
    decomposedDirectories =[]
    for filename in java_files:
        # first we want the name of each directory from dir/dir2/dir3/ .../directoryname
        directory_names = []
        # the regex that breaks down directories of the form dir/dir2/dir3/ .../dirn into a list of each dir name
        found = re.findall('/(.+?)/|(.+?)/|(.+\.java)', filename)
        for tuple in found:
            if tuple[0]:
                directory_names.append(tuple[0])
            if tuple[1]:
                directory_names.append(tuple[1])
            if tuple[2]:
                directory_names.append(tuple[2])

        decomposedDirectories.append((directory_names[5:],filename))
    # you can now calculate structural and semantic scattering between each pair of two files in the repo
    return getStructuralScattering(decomposedDirectories)


def getStructuralScattering(directories):
    # directories is a list of (file name,  directories to the file name lists) tuples
    # calculates the distance between two files. Identify a common folder for each pair
    # of two files and count the number of distinct directories to the common folder in both lists of
    # eg [dira,  dirb, dirc] and [dira, dird, dire], means dira/dirb/dirc and dira/dird/dire
    # using the two lists we can compute structural distance between two files
    filePairs = []
    struct_and_semanic_scattering =[]
    for fileA in directories:
        file1 = fileA[0]
        for fileB in directories[directories.index(fileA)+1:]:
            # Here we compute the similarity between text as semantic similarity
            file_textSimilarity =getTextSimilarity(fileA[1],fileB[1])
            file2 = fileB[0]
            len1 =  len(file1)
            len2 = len(file2)
            if len1 > len2:
              distance =  getDistanceBetweenFiles(file1,file2)
            else:
              distance =  getDistanceBetweenFiles(file2, file1)
            struct_and_semanic_scattering.append((distance,file_textSimilarity))
            filePairs.append((file1[-1], file2[-1]))
    # we finally return file pairs and their related semantic
    # and structural differences for each two pairs of files for the entire repo
    return (filePairs, struct_and_semanic_scattering)


def getDistanceBetweenFiles(file1, file2):
    len1 = len(file1)
    len2 = len(file2)
    for i in range(len2):
        if(file1[i] != file2[i]) and i < len2-2:
            return len(file1[i + 1:]) + len(file2[i + 1:])
        if i == len2-2:
                if file1[i] == file2[i]:
                    return len(file1[i+1:]) -1
                else:
                    return 1 + len(file1[i+1:])


def calculatePeriods(gr): # returns a list of date tuples (start,end) from the first to the last commit date in 3 months intervals
    periodTuple =(list(gr.get_list_commits())[0].author_date, gr.get_head().author_date)
    dates = []
    currentDate = periodTuple[0]
    while (currentDate < periodTuple[1]):
     nextdate= currentDate + relativedelta(months=+3)
     dates.append((currentDate,nextdate))
     currentDate = nextdate
    return dates


def regularExpFinder(commitMessage):
    matched = False
    if (re.search(r'fix(e[ds])?[ \t]*(for)[ ]*?(bugs?)?(defects?)?(pr)?[# \t]*',commitMessage)):
        matched =True
    elif (re.search(r'patch(ed)?[ \t]*(for)[ ]*?(bugs?)?(defects?)?(pr)?[# \t]*', commitMessage)):
        matched =True
    elif (re.search(r'(bugs?|pr|show_bug\\.cgi\\?id=)[# \t]*', commitMessage)):
        matched =True
    return matched


def getTextSimilarity(file1, file2):
    str1 =  open(file1, "r",errors='replace').read()
    str2 = open(file2, "r",errors='replace').read()
    a = set(str1.split())
    b = set(str2.split())
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))


def analyzeCommits(repoPath,gr): # What are the components? File? Methods, Class
    # analyze group commits per period block
    commitIntervals = calculatePeriods(gr)
    # initiate a list to collect of the metrics from each interval/period
    intervalsData = []
    # Find all the buggy and bux fixing commits in the repo. (regex)
    buggy_and_fixing_commits = analyzePastFaults(repoPath, gr)
    faultfixingCommits = buggy_and_fixing_commits[1]
    all_bug_introducing_commits = buggy_and_fixing_commits[0]
    # loop through the commits in each period computing the metrics. append the data to intervalsData
    # Each interval is a tuple,(start date, end date)
    for interval in commitIntervals:
        # dictionery to hold buggy commits and the files they affect
        buggy_commits_dictionery ={}
        # inititate number of commits in the interval to zero
        numberOfcommitsInPeriod = 0
        # a dictionery of each file and the commits that modified it (file -> commit hashes)
        modified_files = {}
        # a dictionery of each files and the developers who workedt on it (file -> developers)
        authors_information ={}
        # a dictionery of each file and the nloc and complexity for the last commit
        # in the interval period (file -> (complexity,nloc))
        lastCommitFileComplexityAndLoc= {}
        #A list of hashes in the interval
        commitHashesInInterval =[]
        # a dictionery of author and the components they authored (author/developer -> files )
        authorEditedComponents = {}
        # loop through the interval filling the above data structs for the period
        for commit in RepositoryMining(repoPath, since=interval[0], to=interval[1],only_modifications_with_file_types=['.java']).traverse_commits(): # specify java
            numberOfcommitsInPeriod += 1
            #print(commit.hash)
            #print(commit.msg)
            developer = commit.author.name
            commitHashesInInterval.append(commit.hash)
            commitModifiedFilenames =[]
            for modified_file in commit.modifications:
                modified_file_name = modified_file.filename
                if re.search(r'\.java', modified_file_name):
                    commitModifiedFilenames.append(modified_file_name)
                    lastCommitFileComplexityAndLoc[modified_file_name] = (modified_file.complexity,modified_file.nloc)
                    if modified_file_name in authors_information:
                        authors_information[modified_file_name].add(developer)

                    else:
                        authors_information[modified_file_name] = {developer}
                    if modified_file_name in modified_files:
                        modified_files[modified_file_name].add(commit.hash)

                    else:
                        modified_files[modified_file_name] = {commit.hash}
            if developer in authorEditedComponents:
                authorEditedComponents[developer] = authorEditedComponents[developer].union(set(commitModifiedFilenames))
            else:
                authorEditedComponents[developer] = set(commitModifiedFilenames)
        # compute past faults for each file in the interval (file -> number of past faults)
        pastFaults = {}
        for file in modified_files.keys():
            number_of_faults = 0
            for hash in modified_files[file]:
                if hash in faultfixingCommits:
                    number_of_faults += 1
            pastFaults[file]= number_of_faults
        # compute the number of buggy files for each commit in the period (commit hash -> files)
        buggy_files = []
        for file in modified_files.keys():
            for hash in modified_files[file]:
                if hash in all_bug_introducing_commits:
                    buggy_files.append(file)
                    if hash in buggy_commits_dictionery:
                        buggy_commits_dictionery[hash].add((file,interval))
                    else:
                        buggy_commits_dictionery[hash] = set([(file,interval)])
        # now you have computed some  metrics and some information needed to compute more metrics.
        # Append that to the intervalsData and move to the next period block

        intervalsData.append((numberOfcommitsInPeriod,modified_files,authors_information,
                              lastCommitFileComplexityAndLoc,authorEditedComponents,
                              commitHashesInInterval,pastFaults,buggy_commits_dictionery,interval,buggy_files))
    # return all the interval's data. this goes to the parallelMetricProcessing function
    return intervalsData


def parallelMetricProcessing(intervalsData, scatteringFilePairData, threads,gr):
    # Each interval data needs some data from the previous interval.
    # For proper parallel mapping, you have to provide that data to the threads
    tupledIntervalsData =[]
    for i in range(len(intervalsData)):
        if i > 0:
         tupledIntervalsData.append((intervalsData[i],intervalsData[i-1][6],i))
        else:
         tupledIntervalsData.append((intervalsData[i], {},i))
    data = map((lambda x: (x,scatteringFilePairData,gr)), tupledIntervalsData)
    pool = ThreadPool(threads)
    pool.map(compute_fileMetrics, data)
    pool.close()
    pool.join()


def compute_fileMetrics(tuple_intervalData):
        # This function takes the intervals (from the current period and some from the previuos period ) data and the
        # entire repo's semantic and structural relationship between each pair of files in the repo

        gr = tuple_intervalData[2]
        intervalData = tuple_intervalData[0][0]
        intervalIndex = tuple_intervalData[0][2]
        scatteringFilePairData = tuple_intervalData[1]
        filename = gr.project_name +" "+ str(intervalIndex)+ str(intervalData[8]) + ".csv"
        fileRows = [["File", "loc", "cyclomatic_Complexity", "number_of_Changes", "number_of_past_faults",
                     "number_of_developers", "change_burst", "change_entropy", "structural_scattering",
                     "semantic_scattering","is_buggy"]]
        '''
        bug_introducing_list = [["buggy_hash","file","period"]]
        buggy_filename = gr.project_name+" "+ "Buggy_files"+str(intervalData[8])+".csv"
        for r in intervalData[7].items():
            bug_introducing_list.append(list((r[0], list(r[1])[0], list(r[1])[1])))
        '''
        for modified_file in intervalData[1]:
            file_Commit_hashes =intervalData[1][modified_file]
            number_of_Changes = len(file_Commit_hashes)
            change_entropy = number_of_Changes / intervalData[0]
            complexity_and_loc_pair= intervalData[3][modified_file]
            cyclomatic_Complexity = complexity_and_loc_pair[0]
            loc = complexity_and_loc_pair[1]
            number_of_past_faults = 0
            if intervalIndex != 0:
               number_of_past_faults = tuple_intervalData[0][1][modified_file]
            developers_list =intervalData[2][modified_file]
            number_of_developers = len(developers_list)
            change_burst = calculateChangeBurst(intervalData[5],file_Commit_hashes)
            # to compute sematic and structural scattering all you have to do is look up
            # the pair of files in the scatteringFilePairData
            (structural_scattering,semantic_scattering) = \
                compute_single_fileScattering(modified_file,developers_list,intervalData[4],scatteringFilePairData)
            is_buggy = 0
            if modified_file in intervalData[9]:
                is_buggy = 1
            fileRows.append([modified_file,loc,cyclomatic_Complexity,number_of_Changes,number_of_past_faults,
                             number_of_developers,change_burst,change_entropy,structural_scattering,semantic_scattering,is_buggy])
            #writeCSV(bug_introducing_list,buggy_filename)
            writeCSV(fileRows,filename)


def compute_single_fileScattering(file_name, authors,authorEditedComponents,scatteringFilePairData):
    # to compute semantic and structural scattering, you will have to look for each author that edited the file
    # for each author, get all other files they edited.
    # for each edited file by the author and your file_name, lookup the pair's metrics in scatteringFilePairData
    # with the list of scattering metrics (semantic and structural), compute accordingly
    authorsSemanticScattering = []
    authorsStructuralScattering = []
    for author in authors:
       semanticScattering = []
       structuralScattering = []
       files_edited_by_author_in_interval = list(authorEditedComponents[author])
       number_of_edited_files = len(list(authorEditedComponents[author]))
       for file1 in files_edited_by_author_in_interval:
           for file2 in files_edited_by_author_in_interval[files_edited_by_author_in_interval.index(file1)+1:]:
               index = -1
               if (file1,file2) in scatteringFilePairData[0]:
                   index = scatteringFilePairData[0].index((file1,file2))
               if (file2,file1) in scatteringFilePairData[0]:
                   index = scatteringFilePairData[0].index((file2,file1))
               if index == -1:
                   semanticScattering.append(0)
                   structuralScattering.append(0)
               else:
                   scatteringData = scatteringFilePairData[1][index]
                   semanticScattering.append(scatteringData[1])
                   structuralScattering.append(scatteringData[0])
       len1 =len(semanticScattering)
       len2 =len(structuralScattering)
       if len1 == 0:
           authorsStructuralScattering.append(0)
       else:
           authorsSemanticScattering.append(number_of_edited_files*(1/(sum(semanticScattering)/len1)))
       if len2 ==0 :
           authorsStructuralScattering.append(0)
       else:
          authorsStructuralScattering.append(number_of_edited_files*((sum(structuralScattering))/len2))
    return (sum(authorsStructuralScattering), sum(authorsSemanticScattering))


def writeCSV(csvData,filename):
    with open(filename, 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(csvData)
    csvFile.close()


def main():
    # main takes a list of repository paths and executes metrics for them in parallel
   pool = ThreadPool(5)
   pool.map(computeRepoMetrics, repoPathList)
   pool.close()
   pool.join()


if __name__ == '__main__':
    main()
