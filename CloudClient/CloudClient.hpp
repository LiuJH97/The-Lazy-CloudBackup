#ifndef __M_CLOUD_H__
#define __M_CLOUD_H__
#include<iostream>
#include<string>
#include<vector>
#include<fstream>
#include<sstream>
#include<thread>
#include<unordered_map>
#include<boost/filesystem.hpp>
#include<boost/algorithm/string.hpp>
#include"httplib.h"

#define CLIENT_BACKUP_DIR "backup"
#define CLIENT_BACKUP_INFO_FILE "back.list"
#define RANGE_MAXSIZE (10 << 20)
#define SERVER_IP "192.168.43.217"
#define SERVER_PORT 9000
#define BACKUP_URL "/list/"
// 客户端功能函数：
// 每次客户端启动，读取备份信息文件，获取已有备份信息(可以使用数据库保存) bool GetBackuoInfo()
// 目录下内容监控(使用boost库实现迭代获取目录下文件名称)                    bool BackupDirListen(const std::string &path)
// 迭代获取文件名                                                           bf::directory_iterator item_begin(file);
//                                                                          bf::directory_iterator item_end;
// 判断文件是否需要备份(根据_backup_list中的备份信息etag与新计算的etag进行比较）         
//                                                                          bool FileIsNeedBackup(const std::string &file)
// 备份文件(多线程分块上传---线程传递多参数遇到了问题,封装一个类，使用一个参数就可以传递所有信息)
//                                                                          bool PutFileData(const std::string &file)
// 添加文件备份信息 (将新计算的etag添加到_backup_list中)                    bool AddBackupInfo(const std::string &file)
// 将所有备份信息刷新存储到文件中                                           bool SetBackupInfo()
// 

namespace bf = boost::filesystem;

class ThrBackUp
{
private:
	std::string _file;
	int64_t _range_start;
	int64_t _range_len;
public:
	bool _res;
public:
	ThrBackUp(const std::string &file, int64_t start, int64_t len) :_res(true),
	 _file(file), _range_start(start), _range_len(len){}
	void Start() {
		//获取文件的range分块数据
		std::ifstream path(_file, std::ios::binary);
		if (!path.is_open()) {
			std::cerr << "range backup file" << _file << " failed\n";
			_res = false;
			return;
		}
		//跳转到range的起始位置
		path.seekg(_range_start, std::ios::beg);
		std::string body;
		body.resize(_range_len);
		//读取文件中range分块的文件数据
		path.read(&body[0], _range_len);
		if (!path.good()) {
			std::cerr << "read file " << _file << " range fata failed\n";
			_res = false;
			return;
		}
		path.close();

		//上传range数据
		bf::path path2(_file);
		//组织上传的url路径 method url version
		//PUT /list/filename HTTP/1.1
		std::string url = BACKUP_URL + path2.filename().string();
		//实例化一个httplib的客户端对象
		httplib::Client cli(SERVER_IP, SERVER_PORT);
		//定义http请求头信息
		httplib::Headers hdr;
		hdr.insert(std::make_pair("Content-Length", std::to_string(_range_len)));
		std::stringstream tmp;
		tmp << "bytes=" << _range_start << "-" << (_range_start + _range_len - 1);
		hdr.insert(std::make_pair("Range", tmp.str().c_str()));
		//通过实例化的Client向服务端发送PUT请求
		auto rsp = cli.Put(url.c_str(), hdr, body, "text/plain");
		if (rsp && rsp->status != 200) {
			_res = false;
		}
		std::stringstream ss;
		ss << "backup file:[" << _file << "] range:[" << _range_start << "-" << _range_len << "]backup success\n";
		std::cout << ss.str();
		return ;
	}
};
class CloudClient
{
private:
	std::unordered_map<std::string, std::string> _backup_list;
public:
	CloudClient() {
		bf::path file(CLIENT_BACKUP_DIR);
		if (!bf::exists(file)) {
			bf::create_directory(file);
		}
	}
private:
	bool GetBackupInfo() {
		bf::path path(CLIENT_BACKUP_INFO_FILE);
		if (bf::exists(path)) {
			std::cerr << "list file" << path.string() << "is not exist\n";
			return false;
		}
		int64_t fsize = bf::file_size(path);
		if (fsize == 0)
		{
			std::cerr << "have no backup info\n";
			return false;
		}
		std::string body;
		body.resize(fsize);
		std::ifstream file(CLIENT_BACKUP_INFO_FILE, std::ios::binary);
		if (!file.is_open()) {
			std::cerr << "list file open error\n";
			return false;
		}
		file.read(&body[0], fsize);
		if (!file.good()) {
			std::cerr << "read list file body error\n";
			return false;
		}

		file.close();

		std::vector<std::string> list;
		boost::split(list, body, boost::is_any_of("\n"));
		for (auto e : list) {
			size_t pos = e.find(" ");
			if (pos == std::string::npos) {
				continue;
			}
			std::string key = e.substr(0, pos);
			std::string val = e.substr(pos + 1);
			_backup_list[key] = val;
		}
		return true;
	}

	bool SetBackupInfo() {
		std::string body;
		for (auto e : _backup_list) {
			body += e.first + " " + e.second + "\n";
		}
		std::ofstream file(CLIENT_BACKUP_INFO_FILE, std::ios::binary);
		if (!file.is_open()) {
			std::cerr << "open list file error\n";
			return false;
		}
		file.write(&body[0], body.size());
		if (!file.good()) {
			std::cerr << "set backup info error\n";
			return false;
		}
		file.close();
	}
	bool BackupDirListen(const std::string &path) {
		bf::path file(path);
		bf::directory_iterator item_begin(file);
		bf::directory_iterator item_end;
		for (; item_begin != item_end; ++item_begin) {
			if (bf::is_directory(item_begin->status())) {
				BackupDirListen(item_begin->path().string());
				continue;
			}
			if (FileIsNeedBackup(item_begin->path().string()) == false) {
				continue;
			}
			std::cerr << "file:[" << item_begin->path().string() << "need backup\n";
 			if (PutFileData(item_begin->path().string()) == false) {
				continue;
			}
			AddBackupInfo(item_begin->path().string());
		}
		return true;
	}

	bool AddBackupInfo(const std::string &file) {
		//etag = "mtime-fsize"
		std::string etag;
		if (GetFileEtage(file, etag) == false) {
			return false;
		}
		_backup_list[file] = etag;
	}

	bool GetFileEtage(const std::string &file) {
		bf::path path(file);
		if (!bf::exists(path)) {
			std::cerr << "get file" << file << " etag error\n";
			return false;
		}
		int64_t fsize = bf::file_size(path);
		int64_t mtime = bf::last_write_time(path);
		std::stringstream tmp;
		tmp << std::hex << fsize << "-" << std::hex << mtime;
		etag = tmp.str();
		return true;
	}

	bool FileIsNeedBackup(const std::string &file) {
		if (GetFileEtage(file, etag) == false) {
			return false;
		}
		auto e = _backup_list.find(file);
		if (e != _backup_list.end() && it->second == etag) {
			return false;
		}
		return true;
	}

	static void thr_start(ThrBackUp *backup_info) {
		backup_info->Start();
		return;
	}

	bool PutFileData(const std::string &file) {
		//按分块传输大小对文件进行分块传输
		//通过获取分块传输是否成功判断整个文件是否成功
		//选择多线程处理
		int64_t fsize = bf::file_size(file);
		if (fsize <= 0) {
			std::cerr << "file " << file << "unnecessary backup\n";
			return false;
		}
		int count = (int)(fsize / RANGE_MAXSIZE);
		std::vector<ThrBackUp>thr_res;
		std::vector<std::thread> thr_list;
		std::cerr << "file:[" << file << "] fsize:[" << fsize << "] count:[" << count + 1 << "]\n";
		for (int i = 0; i <= count; i++) {
			int64_t range_start = i * RANGE_MAXSIZE;
			int64_t range_end = (i + 1) * RANGE_MAXSIZE - 1;
			if (i == count) {
				range_end = fsize - 1;
			}
			int64_t range_len = range_end - range_start + 1;
			ThrBackUp backup_info(file, range_start, range_len);
			std::cerr << "file:[" << file << "] range:[" << range_start << "-" << range_end << "]-" << range_len << "\n";
			thr_res.push_back(backup_info);
		}

		for (int i = 0; i <= count; i++) {
			thr_list.push_back(std::thread(thr_start, &thr_res[i]));
		}
		bool ret = true;
		for (int i = 0; i <= count; i++) {
			thr_list[i].join();
			if (thr_res[i]._res == true) {
				continue;
			}
			ret = false;
		}
		if (ret == false) {
			return false;

		}
		std::cerr << "file:[" << file << "] backup success\n";
		return true;
	}


public:
	bool Start(){
		GetBackupInfo();
		while (1)
		{
			BackupDirListen(CLIENT_BACKUP_DIR);
			SetBackupInfo();
			Sleep(10);
		}
	}
};

#endif