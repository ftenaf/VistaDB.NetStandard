namespace VistaDB.Engine.Internal
{
  internal interface IWordBreaker
  {
    bool IsWordBreaker(string s, int position);

    bool IsStopWord(string word);
  }
}
