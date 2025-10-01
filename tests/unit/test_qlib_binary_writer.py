"""
Unit tests for QlibBinaryWriter and QlibBinaryValidator

Run with: pytest tests/unit/test_qlib_binary_writer.py
"""

import pytest
import struct
import numpy as np
from pathlib import Path
import pandas as pd

from src.transform.qlib_binary_writer import QlibBinaryWriter, QlibBinaryWriterError
from src.transform.qlib_binary_validator import QlibBinaryValidator, QlibBinaryValidatorError
from src.core.config_loader import ConfigLoader


@pytest.fixture
def config():
    """Create test config"""
    return ConfigLoader()


@pytest.fixture
def enriched_root(tmp_path):
    """Create temporary enriched data directory"""
    return tmp_path / 'enriched'


@pytest.fixture
def qlib_root(tmp_path):
    """Create temporary qlib output directory"""
    return tmp_path / 'qlib'


@pytest.fixture
def sample_data(enriched_root):
    """Create sample enriched Parquet data for testing"""
    data_type = 'stocks_daily'
    data_dir = enriched_root / data_type / '2025' / '09'
    data_dir.mkdir(parents=True, exist_ok=True)

    # Create sample data for 2 symbols, 5 trading days
    symbols = ['AAPL', 'MSFT']
    dates = ['2025-09-23', '2025-09-24', '2025-09-25', '2025-09-26', '2025-09-27']

    data = []
    for symbol in symbols:
        for date in dates:
            data.append({
                'symbol': symbol,
                'date': date,
                'open': 150.0 + np.random.randn(),
                'high': 155.0 + np.random.randn(),
                'low': 145.0 + np.random.randn(),
                'close': 150.0 + np.random.randn(),
                'volume': 1000000 + int(np.random.randn() * 10000),
                'vwap': 150.0 + np.random.randn(),
            })

    df = pd.DataFrame(data)
    output_file = data_dir / '2025-09-23.parquet'
    df.to_parquet(output_file, index=False)

    return {
        'data_type': data_type,
        'symbols': symbols,
        'dates': dates,
        'df': df
    }


@pytest.fixture
def writer(enriched_root, qlib_root, config):
    """Create test binary writer"""
    writer = QlibBinaryWriter(enriched_root, qlib_root, config)
    yield writer
    writer.close()


@pytest.fixture
def validator(qlib_root):
    """Create test validator"""
    return QlibBinaryValidator(qlib_root)


def test_writer_initialization(writer, enriched_root, qlib_root):
    """Test QlibBinaryWriter initialization"""
    assert writer.enriched_root == enriched_root
    assert writer.qlib_root == qlib_root
    assert writer.conn is not None


def test_instruments_generation(writer, sample_data):
    """Test instruments file created correctly"""
    data_type = sample_data['data_type']
    output_dir = writer.qlib_root / data_type

    symbols = writer._generate_instruments(data_type, output_dir)

    # Check symbols list
    assert len(symbols) == len(sample_data['symbols'])
    assert set(symbols) == set(sample_data['symbols'])

    # Check instruments file
    instruments_file = output_dir / 'instruments' / 'all.txt'
    assert instruments_file.exists()

    with open(instruments_file) as f:
        file_symbols = [line.strip() for line in f]

    assert file_symbols == sorted(sample_data['symbols'])


def test_calendar_generation(writer, sample_data):
    """Test calendar file with correct trading days"""
    data_type = sample_data['data_type']
    output_dir = writer.qlib_root / data_type

    trading_days = writer._generate_calendar(
        data_type=data_type,
        start_date=sample_data['dates'][0],
        end_date=sample_data['dates'][-1],
        output_dir=output_dir
    )

    # Check trading days list
    assert len(trading_days) == len(sample_data['dates'])
    assert trading_days == sample_data['dates']

    # Check calendar file
    calendar_file = output_dir / 'calendars' / 'day.txt'
    assert calendar_file.exists()

    with open(calendar_file) as f:
        file_dates = [line.strip() for line in f]

    assert file_dates == sample_data['dates']


def test_binary_format(writer, sample_data):
    """Test binary files follow Qlib format"""
    data_type = sample_data['data_type']
    symbol = sample_data['symbols'][0]
    trading_days = sample_data['dates']

    # Get features
    features = writer._get_feature_list(data_type)
    assert len(features) > 0
    assert 'close' in features

    # Convert one symbol
    output_dir = writer.qlib_root / data_type
    result = writer._convert_symbol(
        data_type=data_type,
        symbol=symbol,
        features=features,
        trading_days=trading_days,
        output_dir=output_dir,
        extension='.day.bin'
    )

    # Check result
    assert result['features_written'] > 0
    assert result['bytes_written'] > 0

    # Check binary file format
    binary_file = output_dir / 'features' / symbol.lower() / 'close.day.bin'
    assert binary_file.exists()

    with open(binary_file, 'rb') as f:
        # Read header (4 bytes: count)
        count = struct.unpack('<I', f.read(4))[0]
        assert count == len(trading_days)

        # Read values (count * 4 bytes: float32)
        values = np.fromfile(f, dtype=np.float32, count=count)
        assert len(values) == count


def test_align_to_calendar(writer, sample_data):
    """Test feature alignment to trading calendar"""
    df = sample_data['df']
    symbol_df = df[df['symbol'] == sample_data['symbols'][0]].copy()
    trading_days = sample_data['dates']

    # Align feature
    aligned = writer._align_to_calendar(
        df=symbol_df,
        feature='close',
        trading_days=trading_days
    )

    # Check aligned values
    assert len(aligned) == len(trading_days)
    assert aligned.dtype == np.float32


def test_full_conversion(writer, sample_data):
    """Test full conversion process"""
    data_type = sample_data['data_type']
    start_date = sample_data['dates'][0]
    end_date = sample_data['dates'][-1]

    result = writer.convert_data_type(
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
        incremental=False
    )

    # Check result statistics
    assert result['symbols_converted'] == len(sample_data['symbols'])
    assert result['trading_days'] == len(sample_data['dates'])
    assert result['features_written'] > 0
    assert result['bytes_written'] > 0


def test_validator_instruments(validator, writer, sample_data):
    """Test validator checks instruments file"""
    # First convert data
    data_type = sample_data['data_type']
    writer.convert_data_type(
        data_type=data_type,
        start_date=sample_data['dates'][0],
        end_date=sample_data['dates'][-1],
        incremental=False
    )

    # Validate
    results = validator.validate_conversion(data_type)

    assert results['instruments_valid']
    assert results['symbol_count'] == len(sample_data['symbols'])


def test_validator_calendar(validator, writer, sample_data):
    """Test validator checks calendar file"""
    # First convert data
    data_type = sample_data['data_type']
    writer.convert_data_type(
        data_type=data_type,
        start_date=sample_data['dates'][0],
        end_date=sample_data['dates'][-1],
        incremental=False
    )

    # Validate
    results = validator.validate_conversion(data_type)

    assert results['calendar_valid']
    assert results['trading_days_count'] == len(sample_data['dates'])


def test_validator_features(validator, writer, sample_data):
    """Test validator checks binary features"""
    # First convert data
    data_type = sample_data['data_type']
    writer.convert_data_type(
        data_type=data_type,
        start_date=sample_data['dates'][0],
        end_date=sample_data['dates'][-1],
        incremental=False
    )

    # Validate
    results = validator.validate_conversion(data_type)

    assert results['features_valid']
    assert results['symbols_checked'] > 0
    assert results['features_checked'] > 0


def test_validator_full(validator, writer, sample_data):
    """Test full validation passes"""
    # First convert data
    data_type = sample_data['data_type']
    writer.convert_data_type(
        data_type=data_type,
        start_date=sample_data['dates'][0],
        end_date=sample_data['dates'][-1],
        incremental=False
    )

    # Validate
    results = validator.validate_conversion(data_type)

    assert results['all_valid']
    assert len(results.get('errors', [])) == 0


def test_read_binary_feature(validator, writer, sample_data):
    """Test reading binary feature"""
    # First convert data
    data_type = sample_data['data_type']
    symbol = sample_data['symbols'][0]
    writer.convert_data_type(
        data_type=data_type,
        start_date=sample_data['dates'][0],
        end_date=sample_data['dates'][-1],
        incremental=False
    )

    # Read feature
    values = validator.read_binary_feature(data_type, symbol, 'close')

    assert len(values) == len(sample_data['dates'])
    assert values.dtype == np.float32


def test_get_feature_list(validator, writer, sample_data):
    """Test getting feature list for a symbol"""
    # First convert data
    data_type = sample_data['data_type']
    symbol = sample_data['symbols'][0]
    writer.convert_data_type(
        data_type=data_type,
        start_date=sample_data['dates'][0],
        end_date=sample_data['dates'][-1],
        incremental=False
    )

    # Get features
    features = validator.get_feature_list(data_type, symbol)

    assert len(features) > 0
    assert 'close' in features
    assert 'open' in features


def test_roundtrip_conversion(validator, writer, sample_data):
    """Test Parquet → Binary → read matches original"""
    # Convert data
    data_type = sample_data['data_type']
    symbol = sample_data['symbols'][0]
    writer.convert_data_type(
        data_type=data_type,
        start_date=sample_data['dates'][0],
        end_date=sample_data['dates'][-1],
        incremental=False
    )

    # Get original data
    original_df = sample_data['df'][sample_data['df']['symbol'] == symbol].copy()
    original_df = original_df.sort_values('date').reset_index(drop=True)

    # Read binary feature
    binary_values = validator.read_binary_feature(data_type, symbol, 'close')

    # Compare
    original_values = original_df['close'].values.astype(np.float32)
    assert len(binary_values) == len(original_values)
    np.testing.assert_array_almost_equal(binary_values, original_values, decimal=5)


def test_missing_data(validator):
    """Test validation fails for missing data"""
    results = validator.validate_conversion('nonexistent_data_type')

    assert not results['all_valid']
    assert not results['instruments_valid']
    assert len(results['errors']) > 0


def test_feature_not_found(validator, writer, sample_data):
    """Test error when reading non-existent feature"""
    data_type = sample_data['data_type']
    symbol = sample_data['symbols'][0]

    with pytest.raises(QlibBinaryValidatorError):
        validator.read_binary_feature(data_type, symbol, 'nonexistent_feature')


def test_compare_with_parquet(validator, writer, sample_data):
    """Test comparing binary with Parquet data"""
    # Convert data
    data_type = sample_data['data_type']
    symbol = sample_data['symbols'][0]
    writer.convert_data_type(
        data_type=data_type,
        start_date=sample_data['dates'][0],
        end_date=sample_data['dates'][-1],
        incremental=False
    )

    # Get original data
    original_df = sample_data['df'][sample_data['df']['symbol'] == symbol].copy()
    original_df = original_df.sort_values('date').reset_index(drop=True)

    # Compare
    comparison = validator.compare_with_parquet(
        data_type=data_type,
        symbol=symbol,
        feature='close',
        parquet_df=original_df
    )

    assert comparison['match']
    assert comparison['differences'] == 0
    assert len(comparison.get('errors', [])) == 0
