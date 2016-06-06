#include <cstdio>
#include <limits>
#include <cctype>
#include <algorithm>
#include <memory>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <vector>

using namespace std;

#ifdef _DEBUG
const uint64_t max_input = 1000;
#else
const uint64_t max_input = numeric_limits<uint64_t>::max();
#endif

namespace FileNames
{
  string follower = "follow_info/followers";
  string following = "follow_info/following";
  string forker = "repository_info/forks.txt";
  string language = "repository_info/language.txt";
  string subscriber = "repository_info/subscribers.txt";
}

typedef uint64_t id_t;

string trim(const string &str)
{
  auto s = find_if_not(str.begin(), str.end(), isspace);
  auto e = find_if_not(str.rbegin(), str.rend(), isspace).base();
  if (s <= e)
    return string(s, e);
  return string();
}


struct User
{
  id_t id; // input
  string name; // input

  vector<id_t> followings; // input
  vector<id_t> followers; // input

  vector<id_t> repositories; // input

  vector<id_t> languages; // 소유
  vector<id_t> languages_subscribed; // subscribers 관계
  vector<id_t> languages_forks; // fork 관계
};

void vector_unique(vector<id_t> &vec) {
  sort(vec.begin(), vec.end());
  vec.erase(unique(vec.begin(), vec.end()), vec.end());
}

struct Users : map<id_t, User>
{
  map<string, id_t> nameToId;
};

struct Repository
{
  id_t id; // input
  id_t ownerId; // input

  /* thefron/SNA_Proj이면 owner에 thefron, name에 SNA_Proj가 들어감 */
  string owner; // input
  string name; // input

  id_t language; // input

  vector<id_t> forkers; // input
  vector<id_t> subscribers; // input
};

struct Repositories : map<id_t, Repository>
{
};

struct Languages
{
  map<id_t, string> idToName;
  map<id_t, int64_t> allocateCnt;
  map<string, id_t> nameToId;

  id_t nextId;

  Languages() : nextId(0) {}

  id_t AllocateId(string name)
  {
    if (nameToId.count(name))
    {
      auto res = nameToId[name];
      allocateCnt[res]++;
      return res;
    }
    
    id_t newId = nextId;
    nameToId[name] = newId;
    idToName[newId] = name;
    allocateCnt[newId] = 1;
    nextId++;
    return newId;
  }
};

vector<id_t> parseIdList(istringstream &s)
{
  vector<id_t> res;
  id_t element;
  while (s >> element)
  {
    res.push_back(element);
  }
  return res;
}

void readUserInfo(Users &users)
{
  {
    fprintf(stderr, "opening %s\n", FileNames::follower.c_str());
    ifstream follower(FileNames::follower);
    follower.sync_with_stdio(false);
    if (!follower.is_open())
    {
      fprintf(stderr, "Failed opening follower file\n");
      exit(1);
    }
    uint64_t linecnt = 0;
    string line;
    while (getline(follower, line))
    {
      if (++linecnt >= max_input)
        break;
      line = trim(line);
      /* skip empty line */
      if (line.empty())
        continue;
      istringstream scan(line);
      id_t id;
      string name;
      scan >> id >> name;
      if (scan.fail())
      {
        fprintf(stderr, "warning: invalid line found\n");
      }
      name.shrink_to_fit();
      users.nameToId[name] = id;
      auto &user = users[id];
      user.name = name;
      user.id = id;

      user.followers = parseIdList(scan);

      if (!scan.eof())
      {
        fprintf(stderr, "warning: invalid line found\n");
      }
    }
    if (linecnt < max_input && (follower.bad() || !follower.eof())) {
      fprintf(stderr, "fail reading follower file\n");
      exit(1);
    }
  }


  {
    fprintf(stderr, "opening %s\n", FileNames::following.c_str());
    ifstream following(FileNames::following);
    following.sync_with_stdio(false);
    if (!following.is_open())
    {
      fprintf(stderr, "Failed opening following file\n");
      exit(1);
    }
    uint64_t linecnt = 0;
    string line;
    while (getline(following, line))
    {
      if (++linecnt >= max_input)
        break;
      line = trim(line);
      /* skip empty line */
      if (line.empty())
        continue;
      istringstream scan(line);
      id_t id;
      string name;
      scan >> id >> name;
      if (scan.fail())
      {
        fprintf(stderr, "warning: invalid line found");
      }
      name.shrink_to_fit();
      users.nameToId[name] = id;
      auto &user = users[id];
      user.name = name;

      user.followings = parseIdList(scan);

      if (!scan.eof())
      {
        fprintf(stderr, "warning: invalid line found");
      }
    }
    if (linecnt < max_input && (following.bad() || !following.eof())) {
      fprintf(stderr, "fail reading following file");
      exit(1);
    }
  }
}

tuple<string, string> splitRepoFullName(string fullname)
{
  auto pos = fullname.find('/');
  return make_tuple(fullname.substr(0u, pos), fullname.substr(pos + 1));
}

void readRepositoryInfo(Users &users, Repositories &repos, Languages &languages)
{
  {
    fprintf(stderr, "opening %s\n", FileNames::language.c_str());
    ifstream language(FileNames::language);
    language.sync_with_stdio(false);
    if (!language.is_open())
    {
      fprintf(stderr, "Failed opening language file\n");
      exit(1);
    }
    uint64_t linecnt = 0;
    string line;
    while (getline(language, line))
    {
      if (++linecnt >= max_input)
        break;
      line = trim(line);
      /* skip empty line */
      if (line.empty())
        continue;
      istringstream scan(line);
      id_t id;
      string fullname;
      scan >> id >> fullname;
      if (scan.fail())
      {
        fprintf(stderr, "warning: invalid line found\n");
      }
      string ownerName, repoName;
      tie(ownerName, repoName) = splitRepoFullName(fullname);

      if (users.nameToId.count(ownerName) == 0)
      {
        //fprintf(stderr, "user %s not found (for %s)\n", ownerName.c_str(), fullname.c_str());
        // skip repos whose owner is unknown
        continue;
      }
      id_t ownerId = users.nameToId[ownerName];
      auto &repo = repos[id];
      repo.id = id;
      repo.ownerId = ownerId;

      repo.owner = ownerName;
      repo.name = repoName;

      users[ownerId].repositories.push_back(id);

      string languageName;
      if (!getline(scan, languageName)) {
        fprintf(stderr, "warning: invalid line found");
      }
      repo.language = languages.AllocateId(trim(languageName));
    }
    if (linecnt < max_input && (language.bad() || !language.eof())) {
      fprintf(stderr, "fail reading following file");
      exit(1);
    }
  }


  {
    fprintf(stderr, "opening %s\n", FileNames::forker.c_str());
    ifstream forkerFile(FileNames::forker);
    forkerFile.sync_with_stdio(false);
    if (!forkerFile.is_open())
    {
      fprintf(stderr, "Failed opening forker file\n");
      exit(1);
    }
    uint64_t linecnt = 0;
    string line;
    while (getline(forkerFile, line))
    {
      if (++linecnt >= max_input)
        break;
      line = trim(line);
      /* skip empty line */
      if (line.empty())
        continue;
      istringstream scan(line);
      id_t id;
      string fullname;
      scan >> id >> fullname;
      if (scan.fail())
      {
        fprintf(stderr, "warning: invalid line found\n");
      }
      string ownerName, repoName;
      tie(ownerName, repoName) = splitRepoFullName(fullname);

      /* language not saved */
      if (repos.count(id) == 0)
      {
        //fprintf(stderr, "language unknown: %s\n", fullname.c_str());
        // skip
        continue;
      }
      auto &repo = repos[id];
      repo.forkers = parseIdList(scan);
      if (!scan.eof())
      {
        fprintf(stderr, "warning: invalid line found");
      }
    }
    if (linecnt < max_input && (forkerFile.bad() || !forkerFile.eof())) {
      fprintf(stderr, "fail reading following file");
      exit(1);
    }
  }

  {
    fprintf(stderr, "opening %s\n", FileNames::subscriber.c_str());
    ifstream subscribersFile(FileNames::subscriber);
    subscribersFile.sync_with_stdio(false);
    if (!subscribersFile.is_open())
    {
      fprintf(stderr, "Failed opening subscribers file\n");
      exit(1);
    }
    uint64_t linecnt = 0;
    string line;
    while (getline(subscribersFile, line))
    {
      if (++linecnt >= max_input)
        break;
      line = trim(line);
      /* skip empty line */
      if (line.empty())
        continue;
      istringstream scan(line);
      id_t id;
      string fullname;
      scan >> id >> fullname;
      if (scan.fail())
      {
        fprintf(stderr, "warning: invalid line found\n");
      }
      string ownerName, repoName;
      tie(ownerName, repoName) = splitRepoFullName(fullname);

      /* language not saved */
      if (repos.count(id) == 0)
      {
        //fprintf(stderr, "language unknown: %s\n", fullname.c_str());
        // skip
        continue;
      }
      auto &repo = repos[id];
      repo.subscribers = parseIdList(scan);
      if (!scan.eof())
      {
        fprintf(stderr, "warning: invalid line found");
      }
    }
    if (linecnt < max_input && (subscribersFile.bad() || !subscribersFile.eof())) {
      fprintf(stderr, "fail reading following file");
      exit(1);
    }
  }
}

void saveLanguageStatistics(const Languages &languages)
{
  vector<pair<int64_t, string>> rank;
  for (auto kv : languages.idToName)
  {
    auto id = kv.first;
    auto name = kv.second;
    rank.emplace_back(languages.allocateCnt.find(id)->second, name);
  }
  sort(rank.rbegin(), rank.rend());

  FILE *fp = fopen("langstat.txt", "w");
  fprintf(fp, "# 언어별로 사용된 repository 개수.");
  for (auto kv : rank)
  {
    fprintf(fp, "%s %lld\n", kv.second.c_str(), (long long)kv.first);
  }
  fclose(fp);
}

vector<id_t> vector_union(vector<id_t> a, vector<id_t> b)
{
  a.insert(a.end(), b.begin(), b.end());
  vector_unique(a);
  return a;
}

void saveUserLanguageStatistics(const Users &users, const Languages &languages)
{
  /*
  0: 소유만,
  1: fork 포함
  2: subscribe도 포함
  */
  map<int, long long> 사용언어수[3];
  for (auto &key_user : users)
  {
    auto &user = key_user.second;
    auto lang0 = user.languages;
    auto lang1 = vector_union(lang0, user.languages_forks);
    auto lang2 = vector_union(lang1, user.languages_subscribed);
    사용언어수[0][lang0.size()]++;
    사용언어수[1][lang1.size()]++;
    사용언어수[2][lang2.size()]++;
  }
  {
    FILE *fp = fopen("langcount_per_user.txt", "w");
    int langcnt = languages.idToName.size();
    fprintf(fp, "# 사용언어수가 x개인 사람이 몇 명이나 있는지 카운팅\n");
    fprintf(fp, "# 사용언어수, 소유, 소유+fork, 소유+fork+subscribe\n");
    for (int cnt = 0; cnt <= langcnt; cnt++)
    {
      fprintf(fp, "%d %lld %lld %lld\n", cnt, 사용언어수[0][cnt], 사용언어수[1][cnt], 사용언어수[2][cnt]);
    }
    fclose(fp);
  }
}

void fillLanguages(Users &users, const Repositories &repos, const Languages &languages)
{
  for (auto &key_repo : repos)
  {
    auto repo_id = key_repo.first;
    auto &repo = key_repo.second;
    for (auto user_id : repo.forkers)
    {
      if (users.count(user_id) == 0)
        continue;
      users[user_id].languages_forks.push_back(repo.language);
    }
    for (auto user_id : repo.subscribers)
    {
      if (users.count(user_id) == 0)
        continue;
      users[user_id].languages_subscribed.push_back(repo.language);
    }
  }

  /* simple ownership */
  for (auto &key_user : users)
  {
    auto &user = key_user.second;
    vector<id_t> langset;
    for (auto repo_id : user.repositories)
    {
      auto I = repos.find(repo_id);
      if (I == repos.cend())
        continue;
      langset.push_back(I->second.language);
    }
    vector_unique(langset);
    user.languages = move(langset);
    vector_unique(user.languages_forks);
    vector_unique(user.languages_subscribed);
  }
}

void restoreUserInput(Users &users)
{
  FILE *fp = fopen("user-input.txt", "w");
  for (auto &kv : users)
  {
    fprintf(fp, "%d %s\n", (int)kv.second.id, kv.second.name.c_str());
  }
  fclose(fp);
}

void saveLanguageToUsers(Users &users, Languages &lang)
{
  map<id_t, vector<id_t>> langToUsers[3];
  for (const auto &keyuser : users)
  {
    const auto &user = keyuser.second;
    for (auto lang : user.languages)
    {
      for (int i = 0; i < 3; i++) {
        langToUsers[i][lang].push_back(user.id);
      }
    }
    for (auto lang : user.languages_forks)
    {
      for (int i = 1; i < 3; i++) {
        langToUsers[i][lang].push_back(user.id);
      }
    }
    for (auto lang : user.languages_subscribed)
    {
      for (int i = 2; i < 3; i++) {
        langToUsers[i][lang].push_back(user.id);
      }
    }
  }
  string fnames[3] = {"lang-user_own.txt","lang-user_own+fork.txt","lang-user_own+fork+sub.txt"};
  for (int i = 0; i < 3; i++) {
    FILE *fp = fopen(fnames[i].c_str(), "w");
    for (auto &kv : langToUsers[i])
    {
      fprintf(fp,"%lld <%s>", (long long)kv.first, lang.idToName[kv.first].c_str());
      for (auto id : kv.second)
      {
        fprintf(fp," %lld", (long long)id);
      }
      fprintf(fp, "\n");
    }
    fclose(fp);
  }
}

int main(int argc, char *argv)
{
  Users users;
  Repositories repositories;
  Languages languages;
  readUserInfo(users);
  readRepositoryInfo(users, repositories, languages);
  printf("users: %d\n", (int)users.size());
  printf("repos: %d\n", (int)repositories.size());

  fillLanguages(users, repositories, languages);

  saveLanguageStatistics(languages);
  saveUserLanguageStatistics(users, languages);
  saveLanguageToUsers(users, languages);
  return 0;
}
