-module(rmdir).

-export([path/1]).

-include_lib("kernel/include/file.hrl").

path(Dir) ->
    remove_all_files(".", [Dir]).

remove_all_files(Dir, Files) ->
    lists:foreach(fun(File) ->
                          FilePath = filename:join([Dir, File]),
                          case file:read_file_info(FilePath) of
                              {ok, FileInfo} ->
                                  case FileInfo#file_info.type of
                                      directory ->
                                          {ok, DirFiles} = file:list_dir(FilePath),
                                          remove_all_files(FilePath, DirFiles),
                                          file:del_dir(FilePath);
                                      _ ->
                                          file:delete(FilePath)
                                  end;
                              {error, _Reason} ->
                                  ok
                          end
                  end, Files).
