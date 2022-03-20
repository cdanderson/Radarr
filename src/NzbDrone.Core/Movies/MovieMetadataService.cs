using System.Collections.Generic;

namespace NzbDrone.Core.Movies
{
    public interface IMovieMetadataService
    {
        MovieMetadata Get(int id);
        bool Upsert(MovieMetadata movie);
        bool UpsertMany(List<MovieMetadata> movies);
    }

    public class MovieMetadataService : IMovieMetadataService
    {
        private readonly IMovieMetadataRepository _movieMetadataRepository;

        public MovieMetadataService(IMovieMetadataRepository movieMetadataRepository)
        {
            _movieMetadataRepository = movieMetadataRepository;
        }

        public MovieMetadata Get(int id)
        {
            return _movieMetadataRepository.Get(id);
        }

        public bool Upsert(MovieMetadata movie)
        {
            return _movieMetadataRepository.UpsertMany(new List<MovieMetadata> { movie });
        }

        public bool UpsertMany(List<MovieMetadata> movies)
        {
            return _movieMetadataRepository.UpsertMany(movies);
        }
    }
}
